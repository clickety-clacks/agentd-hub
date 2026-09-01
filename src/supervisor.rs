use crate::discovery::{Deadlines, Programs, unix_time_ms};
use crate::lifecycle::shutdown_requested;
use crate::model::parse_agentd_snapshot;
use crate::state::HubState;
use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, BufReader, Lines};
use tokio::process::{Child, Command};
use tokio::sync::watch;

pub fn spawn_watchers(
    hub: HubState,
    programs: Programs,
    deadlines: Deadlines,
    shutdown: watch::Receiver<bool>,
) -> Vec<tokio::task::JoinHandle<()>> {
    hub.current()
        .sources
        .iter()
        .map(|source| {
            let machine = source.machine.clone();
            let hub = hub.clone();
            let programs = programs.clone();
            let shutdown = shutdown.clone();
            tokio::spawn(async move {
                watch_source(hub, programs, deadlines, machine, shutdown).await;
            })
        })
        .collect()
}

async fn watch_source(
    hub: HubState,
    programs: Programs,
    deadlines: Deadlines,
    machine: String,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut backoff = Backoff::default();
    loop {
        if *shutdown.borrow() {
            return;
        }
        let Some(attempt) =
            watch_attempt(&hub, &programs, deadlines, &machine, &mut shutdown).await
        else {
            return;
        };
        if attempt.accepted_frame {
            backoff.reset();
        }
        match attempt.outcome {
            WatchOutcome::NoAgentd => hub.mark_no_agentd(&machine, unix_time_ms()),
            WatchOutcome::Failed => hub.mark_not_reached(&machine, unix_time_ms()),
        }
        tokio::select! {
            biased;
            _ = shutdown_requested(&mut shutdown) => return,
            _ = tokio::time::sleep(backoff.next_delay()) => {}
        }
    }
}

struct AttemptResult {
    accepted_frame: bool,
    outcome: WatchOutcome,
}

enum WatchOutcome {
    Failed,
    NoAgentd,
}

async fn watch_attempt(
    hub: &HubState,
    programs: &Programs,
    deadlines: Deadlines,
    machine: &str,
    shutdown: &mut watch::Receiver<bool>,
) -> Option<AttemptResult> {
    let child = match spawn_watch(&programs.ssh, machine) {
        Ok(child) => child,
        Err(_) => {
            return Some(AttemptResult {
                accepted_frame: false,
                outcome: WatchOutcome::Failed,
            });
        }
    };
    watch_child(hub, child, deadlines, machine, shutdown).await
}

async fn watch_child(
    hub: &HubState,
    mut child: Child,
    deadlines: Deadlines,
    machine: &str,
    shutdown: &mut watch::Receiver<bool>,
) -> Option<AttemptResult> {
    let stdout = child.stdout.take().expect("watch stdout is piped");
    let mut lines = BufReader::new(stdout).lines();
    let first_frame_deadline = tokio::time::Instant::now() + deadlines.watch_first_frame;
    let mut accepted_frame = false;

    loop {
        let event = if accepted_frame {
            tokio::select! {
                biased;
                _ = shutdown_requested(shutdown) => WatchEvent::Shutdown,
                status = child.wait() => WatchEvent::Exited(status),
                line = lines.next_line() => WatchEvent::Line(line),
            }
        } else {
            tokio::select! {
                biased;
                _ = shutdown_requested(shutdown) => WatchEvent::Shutdown,
                status = child.wait() => WatchEvent::Exited(status),
                line = lines.next_line() => WatchEvent::Line(line),
                _ = tokio::time::sleep_until(first_frame_deadline) => WatchEvent::FirstFrameDeadline,
            }
        };

        match event {
            WatchEvent::Shutdown => {
                terminate_and_reap(&mut child).await;
                return None;
            }
            WatchEvent::Exited(Ok(status)) => {
                if drain_frames(hub, machine, &mut lines, &mut accepted_frame)
                    .await
                    .is_err()
                {
                    return Some(AttemptResult {
                        accepted_frame,
                        outcome: WatchOutcome::Failed,
                    });
                }
                return Some(AttemptResult {
                    accepted_frame,
                    outcome: completed_outcome(accepted_frame, Some(status)),
                });
            }
            WatchEvent::Exited(Err(_)) => {
                terminate_and_reap(&mut child).await;
                return Some(AttemptResult {
                    accepted_frame,
                    outcome: WatchOutcome::Failed,
                });
            }
            WatchEvent::Line(Ok(Some(line))) => {
                if accept_frame(hub, machine, &line, &mut accepted_frame).is_err() {
                    terminate_and_reap(&mut child).await;
                    return Some(AttemptResult {
                        accepted_frame,
                        outcome: WatchOutcome::Failed,
                    });
                }
            }
            WatchEvent::Line(Err(_)) => {
                terminate_and_reap(&mut child).await;
                return Some(AttemptResult {
                    accepted_frame,
                    outcome: WatchOutcome::Failed,
                });
            }
            WatchEvent::Line(Ok(None)) => {
                let status = child.wait().await.ok();
                return Some(AttemptResult {
                    accepted_frame,
                    outcome: completed_outcome(accepted_frame, status),
                });
            }
            WatchEvent::FirstFrameDeadline => {
                terminate_and_reap(&mut child).await;
                let _ = drain_frames(hub, machine, &mut lines, &mut accepted_frame).await;
                return Some(AttemptResult {
                    accepted_frame,
                    outcome: WatchOutcome::Failed,
                });
            }
        }
    }
}

enum WatchEvent {
    Shutdown,
    Exited(std::io::Result<std::process::ExitStatus>),
    Line(std::io::Result<Option<String>>),
    FirstFrameDeadline,
}

fn accept_frame(
    hub: &HubState,
    machine: &str,
    line: &str,
    accepted_frame: &mut bool,
) -> Result<(), ()> {
    let snapshot = parse_agentd_snapshot(line.as_bytes()).map_err(|_| ())?;
    *accepted_frame = true;
    hub.accept_snapshot(machine, snapshot, unix_time_ms());
    Ok(())
}

async fn drain_frames<R: AsyncBufRead + Unpin>(
    hub: &HubState,
    machine: &str,
    lines: &mut Lines<R>,
    accepted_frame: &mut bool,
) -> Result<(), ()> {
    while let Some(line) = lines.next_line().await.map_err(|_| ())? {
        accept_frame(hub, machine, &line, accepted_frame)?;
    }
    Ok(())
}

fn completed_outcome(
    accepted_frame: bool,
    status: Option<std::process::ExitStatus>,
) -> WatchOutcome {
    if !accepted_frame
        && matches!(
            status.and_then(|status| status.code()),
            Some(126) | Some(127)
        )
    {
        WatchOutcome::NoAgentd
    } else {
        WatchOutcome::Failed
    }
}

fn spawn_watch(ssh: &std::path::Path, machine: &str) -> std::io::Result<Child> {
    Command::new(ssh)
        .args([
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=10",
            "-o",
            "ServerAliveInterval=10",
            "-o",
            "ServerAliveCountMax=1",
            machine,
            "agentd",
            "watch",
            "--json",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
}

async fn terminate_and_reap(child: &mut Child) {
    let _ = child.start_kill();
    let _ = child.wait().await;
}

#[derive(Default)]
struct Backoff {
    index: usize,
}

impl Backoff {
    const DELAYS: [Duration; 6] = [
        Duration::from_secs(1),
        Duration::from_secs(2),
        Duration::from_secs(4),
        Duration::from_secs(8),
        Duration::from_secs(16),
        Duration::from_secs(30),
    ];

    fn next_delay(&mut self) -> Duration {
        let delay = Self::DELAYS[self.index];
        self.index = (self.index + 1).min(Self::DELAYS.len() - 1);
        delay
    }

    fn reset(&mut self) {
        self.index = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Health;
    use crate::state::SourceSeed;
    use std::time::Instant;

    fn fake_ssh() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/fake-ssh.sh")
    }

    fn wait_for_file(path: &std::path::Path) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !path.exists() {
            assert!(Instant::now() < deadline, "fake SSH did not report startup");
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    #[test]
    fn reconnect_backoff_is_bounded_and_resets_after_a_valid_frame() {
        let mut backoff = Backoff::default();
        assert_eq!(
            (0..7).map(|_| backoff.next_delay()).collect::<Vec<_>>(),
            [1, 2, 4, 8, 16, 30, 30].map(Duration::from_secs)
        );
        backoff.reset();
        assert_eq!(backoff.next_delay(), Duration::from_secs(1));
    }

    #[tokio::test]
    async fn first_frame_deadline_terminates_and_reaps_the_watch_child() {
        let directory = tempfile::tempdir().unwrap();
        let machine = directory.path().display().to_string();
        let pid_file = directory.path().join("pid");
        std::fs::write(directory.path().join("mode"), "timeout\n").unwrap();
        let hub = HubState::new(
            vec![SourceSeed {
                machine: machine.clone(),
                health: Health::NotReached { since_unix_ms: 1 },
                snapshot: None,
            }],
            1,
        );
        let (_shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let child = spawn_watch(&fake_ssh(), &machine).unwrap();
        wait_for_file(&pid_file);
        let result = watch_child(
            &hub,
            child,
            Deadlines {
                tailscale: Duration::from_secs(1),
                probe: Duration::from_secs(1),
                watch_first_frame: Duration::from_millis(50),
            },
            &machine,
            &mut shutdown_rx,
        )
        .await;
        let result = result.expect("watch attempt should finish before shutdown");
        assert!(!result.accepted_frame);
        assert!(matches!(result.outcome, WatchOutcome::Failed));
        let pid: i32 = std::fs::read_to_string(pid_file)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        assert_eq!(unsafe { libc::kill(pid, 0) }, -1);
    }

    #[tokio::test]
    async fn one_valid_frame_publishes_once_before_exit() {
        let directory = tempfile::tempdir().unwrap();
        let machine = directory.path().display().to_string();
        let started_file = directory.path().join("started");
        let exited_file = directory.path().join("exited");
        std::fs::write(directory.path().join("mode"), "valid\n").unwrap();
        let hub = HubState::new(
            vec![SourceSeed {
                machine: machine.clone(),
                health: Health::NotReached { since_unix_ms: 1 },
                snapshot: None,
            }],
            1,
        );
        let (_shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let child = spawn_watch(&fake_ssh(), &machine).unwrap();
        wait_for_file(&started_file);
        wait_for_file(&exited_file);
        let result = watch_child(
            &hub,
            child,
            Deadlines::default(),
            &machine,
            &mut shutdown_rx,
        )
        .await;
        let result = result.expect("watch attempt should finish before shutdown");
        assert!(result.accepted_frame);
        assert!(matches!(result.outcome, WatchOutcome::Failed));
        assert_eq!(hub.current().revision, 2);
        assert!(matches!(
            hub.current().sources[0].health,
            Health::Reporting { .. }
        ));
    }
}
