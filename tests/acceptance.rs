use std::fs;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

const PRIVATE_SENTINEL: &str = "private-env-sentinel-9f411a";
const STDERR_SENTINEL: &str = "private-ssh-stderr-sentinel-81cb7e";

struct RunningHub {
    child: Child,
    address: String,
}

impl RunningHub {
    fn start(root: &Path) -> Self {
        let reservation = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = reservation.local_addr().unwrap().to_string();
        drop(reservation);
        let path = std::env::var_os("PATH").unwrap_or_default();
        let mut paths = vec![root.join("bin")];
        paths.extend(std::env::split_paths(&path));
        let mut child = Command::new(env!("CARGO_BIN_EXE_agentd-hub"))
            .args([
                "--listen",
                &address,
                "--hosts-file",
                root.join("must-not-read-hosts")
                    .to_str()
                    .expect("temporary path is UTF-8"),
            ])
            .current_dir(root)
            .env("PATH", std::env::join_paths(paths).unwrap())
            .env("PRIVATE_TEST_VALUE", PRIVATE_SENTINEL)
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(4);
        loop {
            if TcpStream::connect(&address).is_ok() {
                break;
            }
            if let Some(status) = child.try_wait().unwrap() {
                let mut stderr = String::new();
                child
                    .stderr
                    .take()
                    .unwrap()
                    .read_to_string(&mut stderr)
                    .unwrap();
                panic!("hub exited before listening: {status}: {stderr}");
            }
            assert!(
                Instant::now() < deadline,
                "hub did not listen before deadline"
            );
            thread::sleep(Duration::from_millis(20));
        }
        Self { child, address }
    }

    fn stop(mut self) {
        let result = unsafe { libc::kill(self.child.id() as i32, libc::SIGINT) };
        assert_eq!(result, 0);
        assert!(self.child.wait().unwrap().success());
    }
}

fn executable(path: &Path, contents: &str) {
    let mut file = fs::File::create(path).unwrap();
    file.write_all(contents.as_bytes()).unwrap();
    file.sync_all().unwrap();
    drop(file);
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
}

fn fixture_root() -> tempfile::TempDir {
    let root = tempfile::tempdir().unwrap();
    let bin = root.path().join("bin");
    fs::create_dir(&bin).unwrap();
    executable(
        &bin.join("tailscale"),
        &format!(
            "#!/bin/sh\necho tailscale >> {}\nprintf '%s\\n' '{{\"Self\":{{\"DNSName\":\"beta.\"}},\"Peer\":{{\"a\":{{\"DNSName\":\"alpha.\"}},\"duplicate\":{{\"DNSName\":\"beta.\"}}}}}}'\n",
            root.path().join("tailscale-calls").display()
        ),
    );
    executable(
        &bin.join("ssh"),
        &format!(
            r##"#!/bin/sh
case " $* " in
  *" agentd list --json "*)
    echo '{stderr_sentinel}' >&2
    machine=unknown
    for arg in "$@"; do case "$arg" in alpha|beta) machine=$arg;; esac; done
    echo "$machine" >> {probe_log}
    printf '{{"type":"snapshot","schema":"agentd.snapshot.v1","instanceId":"instance-%s","revision":3,"observedAtUnixMs":4,"scan":{{"state":"complete","issues":[]}},"agents":[{{"id":{{"pid":7,"startTimeTicks":9}},"harness":"codex","detectedBy":"proc_comm","presence":{{"state":"present","cause":null}},"cwd":{{"state":"known","value":"/work/<script>","cause":null}},"activity":{{"state":"needs_attention","source":"hook","observedAtUnixMs":1}},"tty":"pts/1","tmux":{{"session":"agents","windowIndex":2,"windowName":"<img>","paneId":"%%7"}},"name":"<script>external.example","startedAtUnixMs":1}}]}}\n' "$machine"
    ;;
  *" agentd watch --json "*) exec sleep 30 ;;
  *) exit 255 ;;
esac
"##,
            stderr_sentinel = STDERR_SENTINEL,
            probe_log = root.path().join("probe-calls").display()
        ),
    );
    fs::write(root.path().join("tailscale-calls"), "").unwrap();
    fs::write(root.path().join("probe-calls"), "").unwrap();
    root
}

fn request(address: &str, path: &str, extra_headers: &str, stream: bool) -> String {
    let mut socket = TcpStream::connect(address).unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    write!(
        socket,
        "GET {path} HTTP/1.1\r\nHost: localhost\r\n{extra_headers}{}\r\n",
        if stream { "" } else { "Connection: close\r\n" }
    )
    .unwrap();
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 4096];
    loop {
        match socket.read(&mut buffer) {
            Ok(0) => break,
            Ok(count) => {
                bytes.extend_from_slice(&buffer[..count]);
                if stream && bytes.windows(b"\n\n".len()).any(|window| window == b"\n\n") {
                    break;
                }
            }
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                break;
            }
            Err(error) => panic!("HTTP read failed: {error}"),
        }
    }
    String::from_utf8(bytes).unwrap()
}

fn top_level_names(path: &Path) -> Vec<PathBuf> {
    let mut names: Vec<_> = fs::read_dir(path)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    names.sort();
    names
}

#[test]
fn loopback_snapshot_sse_privacy_routes_and_stateless_restart() {
    let fixture = fixture_root();
    let before = top_level_names(fixture.path());
    let hub = RunningHub::start(fixture.path());

    assert!(TcpStream::connect(&hub.address).is_ok());
    let port = hub.address.rsplit_once(':').unwrap().1;
    assert!(TcpStream::connect(format!("[::1]:{port}")).is_err());

    let snapshot = request(&hub.address, "/snapshot", "", false);
    assert!(
        snapshot.starts_with("HTTP/1.1 200 OK"),
        "unexpected snapshot response: {snapshot:?}"
    );
    assert!(snapshot.contains("content-type: application/json"));
    assert!(snapshot.contains("\"revision\":1"));
    assert!(snapshot.contains("\"machine\":\"alpha\""));
    assert!(snapshot.contains("\"machine\":\"beta\""));
    assert!(snapshot.contains("\"instanceId\":\"instance-alpha\""));
    assert!(snapshot.contains("\"instanceId\":\"instance-beta\""));
    assert!(!snapshot.contains(PRIVATE_SENTINEL));
    assert!(!snapshot.contains(STDERR_SENTINEL));
    assert_eq!(
        fs::read_to_string(fixture.path().join("tailscale-calls")).unwrap(),
        "tailscale\n"
    );
    assert_eq!(
        fs::read_to_string(fixture.path().join("probe-calls")).unwrap(),
        "alpha\nbeta\n"
    );

    let events = request(&hub.address, "/events", "Last-Event-ID: 999\r\n", true);
    assert!(events.starts_with("HTTP/1.1 200 OK"));
    assert!(events.contains("content-type: text/event-stream"));
    assert!(events.contains("event: snapshot"));
    assert!(events.contains("id: 1"));
    assert!(events.contains("\"machine\":\"alpha\""));
    assert!(!events.contains(PRIVATE_SENTINEL));
    assert!(!events.contains(STDERR_SENTINEL));

    let root = request(&hub.address, "/", "", false);
    assert!(root.contains("content-type: text/html; charset=utf-8"));
    assert!(root.contains("new EventSource('/events')"));
    assert!(!root.contains(PRIVATE_SENTINEL));
    assert!(!root.contains(STDERR_SENTINEL));

    let unknown = request(&hub.address, "/unknown", "", false);
    assert!(unknown.starts_with("HTTP/1.1 404 Not Found"));
    let mut socket = TcpStream::connect(&hub.address).unwrap();
    socket
        .write_all(b"POST /snapshot HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .unwrap();
    let mut post = String::new();
    socket.read_to_string(&mut post).unwrap();
    assert!(post.starts_with("HTTP/1.1 405 Method Not Allowed"));
    assert!(request(&hub.address, "/snapshot", "", false).contains("\"revision\":1"));

    hub.stop();
    assert_eq!(top_level_names(fixture.path()), before);

    let restarted = RunningHub::start(fixture.path());
    assert!(request(&restarted.address, "/snapshot", "", false).contains("\"revision\":1"));
    restarted.stop();
    assert_eq!(top_level_names(fixture.path()), before);
}

#[test]
fn non_loopback_listener_is_rejected_before_discovery_or_ssh() {
    let fixture = fixture_root();
    let marker = fixture.path().join("discovery-started");
    executable(
        &fixture.path().join("bin/tailscale"),
        &format!("#!/bin/sh\ntouch {}\n", marker.display()),
    );
    let path = std::env::var_os("PATH").unwrap_or_default();
    let mut paths = vec![fixture.path().join("bin")];
    paths.extend(std::env::split_paths(&path));
    let output = Command::new(env!("CARGO_BIN_EXE_agentd-hub"))
        .args(["--listen", "0.0.0.0:8787"])
        .env("PATH", std::env::join_paths(paths).unwrap())
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("non_loopback_listen_refused"));
    assert!(!marker.exists());
}
