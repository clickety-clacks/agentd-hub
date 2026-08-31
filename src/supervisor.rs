use crate::discovery::{Deadlines, Programs, unix_time_ms};
use crate::lifecycle::shutdown_requested;
use crate::model::parse_agentd_snapshot;
use crate::state::HubState;
use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
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
    let mut child = match spawn_watch(&programs.ssh, machine) {
        Ok(child) => child,
        Err(_) => {
            return Some(AttemptResult {
                accepted_frame: false,
                outcome: WatchOutcome::Failed,
            });
        }
    };
    let stdout = child.stdout.take().expect("watch stdout is piped");
    let mut lines = BufReader::new(stdout).lines();
    let first_frame = tokio::time::sleep(deadlines.watch_first_frame);
    tokio::pin!(first_frame);
    let mut accepted_frame = false;

    loop {
        let next_line = if accepted_frame {
            tokio::select! {
                biased;
                _ = shutdown_requested(shutdown) => {
                    terminate_and_reap(&mut child).await;
                    return None;
                }
                line = lines.next_line() => line,
            }
        } else {
            let line = tokio::select! {
                biased;
                _ = shutdown_requested(shutdown) => {
                    terminate_and_reap(&mut child).await;
                    return None;
                }
                line = tokio::time::timeout_at(first_frame.deadline(), lines.next_line()) => line,
            };
            match line {
                Ok(line) => line,
                Err(_) => {
                    terminate_and_reap(&mut child).await;
                    return Some(AttemptResult {
                        accepted_frame,
                        outcome: WatchOutcome::Failed,
                    });
                }
            }
        };
        match next_line {
            Ok(Some(line)) => match parse_agentd_snapshot(line.as_bytes()) {
                Ok(snapshot) => {
                    accepted_frame = true;
                    hub.accept_snapshot(machine, snapshot, unix_time_ms());
                }
                Err(_) => {
                    terminate_and_reap(&mut child).await;
                    return Some(AttemptResult {
                        accepted_frame,
                        outcome: WatchOutcome::Failed,
                    });
                }
            },
            Err(_) => {
                terminate_and_reap(&mut child).await;
                return Some(AttemptResult {
                    accepted_frame,
                    outcome: WatchOutcome::Failed,
                });
            }
            Ok(None) => {
                let status = child.wait().await.ok();
                let outcome = if !accepted_frame
                    && matches!(
                        status.and_then(|status| status.code()),
                        Some(126) | Some(127)
                    ) {
                    WatchOutcome::NoAgentd
                } else {
                    WatchOutcome::Failed
                };
                return Some(AttemptResult {
                    accepted_frame,
                    outcome,
                });
            }
        }
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
    use std::io::Write;
    use std::os::unix::fs::PermissionsExt;

    fn executable(path: &std::path::Path, contents: &str) {
        let mut file = std::fs::File::create(path).unwrap();
        file.write_all(contents.as_bytes()).unwrap();
        file.sync_all().unwrap();
        drop(file);
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
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
        let ssh = directory.path().join("ssh");
        let pid_file = directory.path().join("pid");
        executable(
            &ssh,
            &format!(
                "#!/bin/sh\necho $$ > {}\nexec sleep 30\n",
                pid_file.display()
            ),
        );
        let hub = HubState::new(
            vec![SourceSeed {
                machine: "a".into(),
                health: Health::NotReached { since_unix_ms: 1 },
                snapshot: None,
            }],
            1,
        );
        let (_shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let result = watch_attempt(
            &hub,
            &Programs {
                tailscale: "unused".into(),
                ssh,
            },
            Deadlines {
                tailscale: Duration::from_secs(1),
                probe: Duration::from_secs(1),
                watch_first_frame: Duration::from_millis(50),
            },
            "a",
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
        let ssh = directory.path().join("ssh");
        executable(
            &ssh,
            "#!/bin/sh\nprintf '%s\\n' '{\"type\":\"snapshot\",\"schema\":\"agentd.snapshot.v1\",\"instanceId\":\"i\",\"revision\":1,\"observedAtUnixMs\":1,\"scan\":{},\"agents\":[]}'\n",
        );
        let hub = HubState::new(
            vec![SourceSeed {
                machine: "a".into(),
                health: Health::NotReached { since_unix_ms: 1 },
                snapshot: None,
            }],
            1,
        );
        let (_shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let result = watch_attempt(
            &hub,
            &Programs {
                tailscale: "unused".into(),
                ssh,
            },
            Deadlines::default(),
            "a",
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
