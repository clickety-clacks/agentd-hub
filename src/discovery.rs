use crate::lifecycle::shutdown_requested;
use crate::model::{Health, parse_agentd_snapshot};
use crate::state::SourceSeed;
use serde_json::Value;
use std::fmt;
use std::path::{Path, PathBuf};
use std::process::ExitStatus;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio::sync::watch;

#[derive(Clone, Debug)]
pub struct Programs {
    pub tailscale: PathBuf,
    pub ssh: PathBuf,
}

impl Default for Programs {
    fn default() -> Self {
        Self {
            tailscale: "tailscale".into(),
            ssh: "ssh".into(),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Deadlines {
    pub tailscale: Duration,
    pub probe: Duration,
    pub watch_first_frame: Duration,
}

impl Default for Deadlines {
    fn default() -> Self {
        Self {
            tailscale: Duration::from_secs(10),
            probe: Duration::from_secs(10),
            watch_first_frame: Duration::from_secs(15),
        }
    }
}

#[derive(Debug)]
pub enum DiscoveryError {
    NoTargets,
    Shutdown,
    HostsFile {
        path: PathBuf,
        source: std::io::Error,
    },
}

impl fmt::Display for DiscoveryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Shutdown => write!(formatter, "discovery_cancelled"),
            Self::NoTargets => write!(
                formatter,
                "discovery_no_targets: Tailscale yielded no machine and no hosts-file target was available"
            ),
            Self::HostsFile { path, source } => write!(
                formatter,
                "hosts_file_unreadable: {}: {source}",
                path.display()
            ),
        }
    }
}

impl std::error::Error for DiscoveryError {}

pub async fn discover(
    programs: &Programs,
    deadlines: Deadlines,
    hosts_file: Option<&Path>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<Vec<SourceSeed>, DiscoveryError> {
    let tailscale = run_command(
        &programs.tailscale,
        &["status".into(), "--json".into()],
        deadlines.tailscale,
        &mut shutdown,
    )
    .await;
    let targets = match tailscale {
        Ok(CommandOutcome::Shutdown) => return Err(DiscoveryError::Shutdown),
        Ok(CommandOutcome::Completed(output)) if output.status.success() && !output.timed_out => {
            tailscale_targets(&output.stdout).unwrap_or_default()
        }
        _ => Vec::new(),
    };
    let targets = if targets.is_empty() {
        match hosts_file {
            Some(path) => {
                let bytes =
                    tokio::fs::read(path)
                        .await
                        .map_err(|source| DiscoveryError::HostsFile {
                            path: path.to_owned(),
                            source,
                        })?;
                hosts_targets(&bytes)
            }
            None => Vec::new(),
        }
    } else {
        targets
    };
    if targets.is_empty() {
        return Err(DiscoveryError::NoTargets);
    }

    let mut seeds = Vec::with_capacity(targets.len());
    for machine in targets {
        seeds.push(probe_source(programs, deadlines.probe, machine, &mut shutdown).await?);
    }
    Ok(seeds)
}

pub fn tailscale_targets(bytes: &[u8]) -> Option<Vec<String>> {
    let value: Value = serde_json::from_slice(bytes).ok()?;
    let mut targets = Vec::new();
    if let Some(name) = value
        .get("Self")
        .and_then(|entry| entry.get("DNSName"))
        .and_then(Value::as_str)
    {
        push_dns_name(&mut targets, name);
    }
    if let Some(peers) = value.get("Peer").and_then(Value::as_object) {
        for peer in peers.values() {
            if let Some(name) = peer.get("DNSName").and_then(Value::as_str) {
                push_dns_name(&mut targets, name);
            }
        }
    }
    targets.sort();
    targets.dedup();
    Some(targets)
}

fn push_dns_name(targets: &mut Vec<String>, name: &str) {
    let name = name.strip_suffix('.').unwrap_or(name);
    if !name.is_empty() {
        targets.push(name.to_owned());
    }
}

pub fn hosts_targets(bytes: &[u8]) -> Vec<String> {
    let Ok(text) = std::str::from_utf8(bytes) else {
        return Vec::new();
    };
    let mut targets: Vec<_> = text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(str::to_owned)
        .collect();
    targets.sort();
    targets.dedup();
    targets
}

async fn probe_source(
    programs: &Programs,
    deadline: Duration,
    machine: String,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<SourceSeed, DiscoveryError> {
    let args = vec![
        "-o".into(),
        "BatchMode=yes".into(),
        "-o".into(),
        "ConnectTimeout=10".into(),
        machine.clone(),
        AgentdCommand::List.remote_shell().into(),
    ];
    let result = run_command(&programs.ssh, &args, deadline, shutdown).await;
    let now = unix_time_ms();
    let seed = match result {
        Ok(CommandOutcome::Shutdown) => return Err(DiscoveryError::Shutdown),
        Ok(CommandOutcome::Completed(output)) if !output.timed_out => {
            if let Ok(snapshot) = parse_single_frame(&output.stdout) {
                SourceSeed {
                    machine,
                    health: Health::Reporting {
                        observed_at_unix_ms: now,
                    },
                    snapshot: Some(snapshot),
                }
            } else if matches!(output.status.code(), Some(126) | Some(127)) {
                SourceSeed {
                    machine,
                    health: Health::NoAgentd {
                        observed_at_unix_ms: now,
                    },
                    snapshot: None,
                }
            } else {
                SourceSeed {
                    machine,
                    health: Health::NotReached { since_unix_ms: now },
                    snapshot: None,
                }
            }
        }
        _ => SourceSeed {
            machine,
            health: Health::NotReached { since_unix_ms: now },
            snapshot: None,
        },
    };
    Ok(seed)
}

#[derive(Clone, Copy)]
pub(crate) enum AgentdCommand {
    List,
    Watch,
}

impl AgentdCommand {
    pub(crate) fn remote_shell(self) -> &'static str {
        match self {
            Self::List => r#"PATH="$HOME/.local/bin:$PATH" agentd list --json"#,
            Self::Watch => r#"PATH="$HOME/.local/bin:$PATH" agentd watch --json"#,
        }
    }
}

pub fn parse_single_frame(bytes: &[u8]) -> Result<crate::model::AgentdSnapshot, String> {
    let mut frames = bytes
        .split(|byte| *byte == b'\n')
        .map(trim_ascii)
        .filter(|line| !line.is_empty());
    let frame = frames
        .next()
        .ok_or_else(|| "empty Agentd output".to_owned())?;
    if frames.next().is_some() {
        return Err("Agentd list returned more than one frame".into());
    }
    parse_agentd_snapshot(frame)
}

fn trim_ascii(mut bytes: &[u8]) -> &[u8] {
    while bytes.first().is_some_and(u8::is_ascii_whitespace) {
        bytes = &bytes[1..];
    }
    while bytes.last().is_some_and(u8::is_ascii_whitespace) {
        bytes = &bytes[..bytes.len() - 1];
    }
    bytes
}

pub struct CommandOutput {
    pub status: ExitStatus,
    pub stdout: Vec<u8>,
    pub timed_out: bool,
}

pub enum CommandOutcome {
    Completed(CommandOutput),
    Shutdown,
}

pub async fn run_command(
    program: &Path,
    args: &[String],
    deadline: Duration,
    shutdown: &mut watch::Receiver<bool>,
) -> std::io::Result<CommandOutcome> {
    if *shutdown.borrow() {
        return Ok(CommandOutcome::Shutdown);
    }
    let mut child = Command::new(program)
        .args(args)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()?;
    let mut stdout = child.stdout.take().expect("piped stdout");
    let stdout_task = tokio::spawn(async move {
        let mut bytes = Vec::new();
        stdout.read_to_end(&mut bytes).await.map(|_| bytes)
    });
    let wait_result = tokio::select! {
        biased;
        _ = shutdown_requested(shutdown) => None,
        result = tokio::time::timeout(deadline, child.wait()) => Some(result),
    };
    let Some(wait_result) = wait_result else {
        if child.try_wait()?.is_none() {
            child.start_kill()?;
        }
        let _ = child.wait().await?;
        let _ = stdout_task.await.map_err(std::io::Error::other)??;
        return Ok(CommandOutcome::Shutdown);
    };
    let (status, timed_out) = match wait_result {
        Ok(result) => (result?, false),
        Err(_) => {
            child.start_kill()?;
            (child.wait().await?, true)
        }
    };
    let stdout = stdout_task.await.map_err(std::io::Error::other)??;
    Ok(CommandOutcome::Completed(CommandOutput {
        status,
        stdout,
        timed_out,
    }))
}

pub fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .as_millis()
        .try_into()
        .expect("Unix time exceeds u64 milliseconds")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    use tokio::time::Duration;

    fn executable(path: &Path, contents: &str) {
        use std::io::Write;
        let mut file = std::fs::File::create(path).unwrap();
        file.write_all(contents.as_bytes()).unwrap();
        file.sync_all().unwrap();
        drop(file);
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    #[test]
    fn parses_sorted_unique_tailscale_and_hosts_targets() {
        let tailscale = br#"{"Self":{"DNSName":"z.ts.net."},"Peer":{"a":{"DNSName":"a.ts.net."},"z":{"DNSName":"z.ts.net."}}}"#;
        assert_eq!(
            tailscale_targets(tailscale).unwrap(),
            vec!["a.ts.net", "z.ts.net"]
        );
        assert_eq!(
            hosts_targets(b" z-host \n# comment\na-host\n\na-host\n"),
            vec!["a-host", "z-host"]
        );
    }

    #[test]
    fn remote_agentd_commands_are_literal_and_closed() {
        assert_eq!(
            AgentdCommand::List.remote_shell(),
            r#"PATH="$HOME/.local/bin:$PATH" agentd list --json"#
        );
        assert_eq!(
            AgentdCommand::Watch.remote_shell(),
            r#"PATH="$HOME/.local/bin:$PATH" agentd watch --json"#
        );
        assert!(!AgentdCommand::List.remote_shell().contains("/home/"));
        assert!(!AgentdCommand::Watch.remote_shell().contains("/home/"));
    }

    #[tokio::test]
    async fn whole_command_deadline_terminates_and_reaps_child() {
        let directory = tempfile::tempdir().unwrap();
        let script = directory.path().join("hang");
        let pid_file = directory.path().join("pid");
        executable(
            &script,
            &format!(
                "#!/bin/sh\necho $$ > {}\nexec sleep 30\n",
                pid_file.display()
            ),
        );
        let (_shutdown_tx, mut shutdown_rx) = crate::lifecycle::shutdown_channel();
        let outcome = run_command(&script, &[], Duration::from_millis(50), &mut shutdown_rx)
            .await
            .unwrap();
        let CommandOutcome::Completed(output) = outcome else {
            panic!("command shut down before its deadline")
        };
        assert!(output.timed_out);
        let pid: i32 = std::fs::read_to_string(pid_file)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        assert_eq!(unsafe { libc::kill(pid, 0) }, -1);
    }

    #[tokio::test]
    async fn discovery_keeps_reporting_unreachable_and_no_agentd_sources() {
        let directory = tempfile::tempdir().unwrap();
        let tailscale = directory.path().join("tailscale");
        let ssh = directory.path().join("ssh");
        executable(
            &tailscale,
            "#!/bin/sh\nprintf '%s\\n' '{\"Self\":{\"DNSName\":\"reporting.\"},\"Peer\":{\"a\":{\"DNSName\":\"unreachable.\"},\"b\":{\"DNSName\":\"no-agentd.\"}}}'\n",
        );
        executable(
            &ssh,
            r##"#!/bin/sh
case " $* " in
  *" reporting "*)
    printf '%s\n' '{"type":"snapshot","schema":"agentd.snapshot.v1","instanceId":"i","revision":1,"observedAtUnixMs":1,"scan":{"state":"complete","issues":[]},"agents":[{"id":{"pid":7,"startTimeTicks":9},"harness":"codex","detectedBy":"proc_comm","presence":{"state":"present","cause":null},"cwd":{"state":"known","value":"/work","cause":null},"activity":{"state":"unknown","source":"none","observedAtUnixMs":null}}]}'
    ;;
  *" no-agentd "*) echo 'private-stderr-sentinel' >&2; exit 127 ;;
  *) exit 255 ;;
esac
"##,
        );
        let programs = Programs { tailscale, ssh };
        let (_shutdown_tx, shutdown_rx) = crate::lifecycle::shutdown_channel();
        let seeds = discover(
            &programs,
            Deadlines {
                tailscale: Duration::from_secs(5),
                probe: Duration::from_secs(5),
                watch_first_frame: Duration::from_secs(1),
            },
            None,
            shutdown_rx,
        )
        .await
        .unwrap();
        assert_eq!(
            seeds
                .iter()
                .map(|seed| seed.machine.as_str())
                .collect::<Vec<_>>(),
            ["no-agentd", "reporting", "unreachable"]
        );
        assert!(matches!(seeds[0].health, Health::NoAgentd { .. }));
        assert!(matches!(seeds[1].health, Health::Reporting { .. }));
        assert!(matches!(seeds[2].health, Health::NotReached { .. }));
        assert!(seeds[1].snapshot.is_some());
        let hub = crate::state::HubState::new(seeds, 99);
        let initial = hub.current();
        assert_eq!(initial.revision, 1);
        assert_eq!(initial.sources.len(), 3);
        assert_eq!(initial.agents.len(), 1);
        assert!(
            !serde_json::to_string(&*initial)
                .unwrap()
                .contains("private-stderr-sentinel")
        );
    }

    #[tokio::test]
    async fn timed_out_tailscale_falls_back_to_sorted_unique_hosts() {
        let directory = tempfile::tempdir().unwrap();
        let tailscale = directory.path().join("tailscale");
        let ssh = directory.path().join("ssh");
        let log = directory.path().join("probes");
        let hosts = directory.path().join("hosts");
        executable(&tailscale, "#!/bin/sh\nexec sleep 30\n");
        executable(
            &ssh,
            &format!(
                "#!/bin/sh\nfor arg in \"$@\"; do case \"$arg\" in a|b) echo \"$arg\" >> {};; esac; done\nexit 255\n",
                log.display()
            ),
        );
        std::fs::write(&hosts, "b\n# ignored\na\na\n").unwrap();
        let programs = Programs { tailscale, ssh };
        let (_shutdown_tx, shutdown_rx) = crate::lifecycle::shutdown_channel();
        let seeds = discover(
            &programs,
            Deadlines {
                tailscale: Duration::from_millis(50),
                probe: Duration::from_secs(1),
                watch_first_frame: Duration::from_secs(1),
            },
            Some(&hosts),
            shutdown_rx,
        )
        .await
        .unwrap();
        assert_eq!(
            seeds
                .iter()
                .map(|seed| seed.machine.as_str())
                .collect::<Vec<_>>(),
            ["a", "b"]
        );
        assert_eq!(std::fs::read_to_string(log).unwrap(), "a\nb\n");
    }
}
