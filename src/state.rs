use crate::model::{AgentdSnapshot, Health, HubAgent, HubSnapshot, SourceRow};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use tokio::sync::watch;

#[derive(Clone, Debug)]
pub struct SourceSeed {
    pub machine: String,
    pub health: Health,
    pub snapshot: Option<AgentdSnapshot>,
}

#[derive(Clone)]
pub struct HubState {
    inner: Arc<HubInner>,
}

struct HubInner {
    mutable: Mutex<MutableState>,
    snapshots: watch::Sender<Arc<HubSnapshot>>,
}

struct MutableState {
    revision: u64,
    observed_at_unix_ms: u64,
    sources: BTreeMap<String, SourceState>,
}

struct SourceState {
    health: Health,
    snapshot: Option<AgentdSnapshot>,
}

impl HubState {
    pub fn new(seeds: Vec<SourceSeed>, observed_at_unix_ms: u64) -> Self {
        let sources = seeds
            .into_iter()
            .map(|seed| {
                (
                    seed.machine,
                    SourceState {
                        health: seed.health,
                        snapshot: seed.snapshot,
                    },
                )
            })
            .collect();
        let mutable = MutableState {
            revision: 1,
            observed_at_unix_ms,
            sources,
        };
        let initial = Arc::new(mutable.snapshot());
        let (snapshots, _) = watch::channel(initial);
        Self {
            inner: Arc::new(HubInner {
                mutable: Mutex::new(mutable),
                snapshots,
            }),
        }
    }

    pub fn current(&self) -> Arc<HubSnapshot> {
        self.inner.snapshots.borrow().clone()
    }

    pub fn subscribe(&self) -> watch::Receiver<Arc<HubSnapshot>> {
        let _state = self.inner.mutable.lock().expect("hub state poisoned");
        self.inner.snapshots.subscribe()
    }

    pub fn accept_snapshot(
        &self,
        machine: &str,
        snapshot: AgentdSnapshot,
        observed_at_unix_ms: u64,
    ) {
        let mut state = self.inner.mutable.lock().expect("hub state poisoned");
        let source = state
            .sources
            .get_mut(machine)
            .expect("watch source must exist in initial state");
        source.snapshot = Some(snapshot);
        source.health = Health::Reporting {
            observed_at_unix_ms,
        };
        Self::publish(&self.inner, &mut state, observed_at_unix_ms);
    }

    pub fn mark_not_reached(&self, machine: &str, observed_at_unix_ms: u64) {
        let mut state = self.inner.mutable.lock().expect("hub state poisoned");
        let source = state
            .sources
            .get_mut(machine)
            .expect("watch source must exist in initial state");
        if matches!(source.health, Health::NotReached { .. }) {
            return;
        }
        source.health = Health::NotReached {
            since_unix_ms: observed_at_unix_ms,
        };
        Self::publish(&self.inner, &mut state, observed_at_unix_ms);
    }

    pub fn mark_no_agentd(&self, machine: &str, observed_at_unix_ms: u64) {
        let mut state = self.inner.mutable.lock().expect("hub state poisoned");
        let source = state
            .sources
            .get_mut(machine)
            .expect("watch source must exist in initial state");
        if matches!(source.health, Health::NoAgentd { .. }) && source.snapshot.is_none() {
            return;
        }
        source.snapshot = None;
        source.health = Health::NoAgentd {
            observed_at_unix_ms,
        };
        Self::publish(&self.inner, &mut state, observed_at_unix_ms);
    }

    fn publish(inner: &HubInner, state: &mut MutableState, observed_at_unix_ms: u64) {
        state.revision = state
            .revision
            .checked_add(1)
            .expect("hub revision exhausted");
        state.observed_at_unix_ms = observed_at_unix_ms;
        inner.snapshots.send_replace(Arc::new(state.snapshot()));
    }
}

impl MutableState {
    fn snapshot(&self) -> HubSnapshot {
        let mut sources = Vec::with_capacity(self.sources.len());
        let mut agents = Vec::new();
        for (machine, source) in &self.sources {
            let (instance_id, source_revision, source_observed_at_unix_ms, scan) =
                match &source.snapshot {
                    Some(snapshot) => {
                        for agent in &snapshot.agents {
                            agents.push(agent.project(machine, &snapshot.instance_id));
                        }
                        (
                            Some(snapshot.instance_id.clone()),
                            Some(snapshot.revision),
                            Some(snapshot.observed_at_unix_ms),
                            Some(snapshot.scan.clone()),
                        )
                    }
                    None => (None, None, None, None),
                };
            sources.push(SourceRow {
                machine: machine.clone(),
                health: source.health.clone(),
                instance_id,
                source_revision,
                source_observed_at_unix_ms,
                scan,
            });
        }
        agents.sort_by(agent_sort);
        HubSnapshot {
            frame_type: "snapshot",
            schema: "agentd-hub.snapshot.v1",
            revision: self.revision,
            observed_at_unix_ms: self.observed_at_unix_ms,
            sources,
            agents,
        }
    }
}

fn agent_sort(left: &HubAgent, right: &HubAgent) -> std::cmp::Ordering {
    (
        &left.machine,
        &left.instance_id,
        left.id.pid,
        left.id.start_time_ticks,
    )
        .cmp(&(
            &right.machine,
            &right.instance_id,
            right.id.pid,
            right.id.start_time_ticks,
        ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::parse_agentd_snapshot;
    use serde_json::Value;

    fn snapshot(instance: &str, pid: u32, activity: &str) -> AgentdSnapshot {
        let value = format!(
            r#"{{"type":"snapshot","schema":"agentd.snapshot.v1","instanceId":"{instance}","revision":4,"observedAtUnixMs":8,"scan":{{"state":"complete","issues":[]}},"agents":[{{"id":{{"pid":{pid},"startTimeTicks":9}},"harness":"codex","detectedBy":"proc_comm","presence":{{"state":"present","cause":null}},"cwd":{{"state":"known","value":"/work","cause":null}},"activity":{{"state":"{activity}","source":"hook","observedAtUnixMs":5}}}}]}}"#
        );
        parse_agentd_snapshot(value.as_bytes()).unwrap()
    }

    #[test]
    fn identity_isolated_by_machine_and_state_changes_are_atomic() {
        let hub = HubState::new(
            vec![
                SourceSeed {
                    machine: "a".into(),
                    health: Health::Reporting {
                        observed_at_unix_ms: 10,
                    },
                    snapshot: Some(snapshot("same", 7, "active")),
                },
                SourceSeed {
                    machine: "b".into(),
                    health: Health::Reporting {
                        observed_at_unix_ms: 10,
                    },
                    snapshot: Some(snapshot("same", 7, "active")),
                },
            ],
            10,
        );
        let initial = hub.current();
        assert_eq!(initial.revision, 1);
        assert_eq!(initial.agents.len(), 2);
        assert_ne!(initial.agents[0].machine, initial.agents[1].machine);

        hub.accept_snapshot("a", snapshot("new", 8, "needs_attention"), 20);
        let changed = hub.current();
        assert_eq!(changed.revision, 2);
        assert_eq!(changed.observed_at_unix_ms, 20);
        assert_eq!(changed.sources[0].instance_id.as_deref(), Some("new"));
        assert_eq!(changed.agents[0].id.pid, 8);
    }

    #[test]
    fn duplicate_valid_frames_increment_and_stale_agents_are_retained() {
        let seed = snapshot("i", 7, "needs_attention");
        let hub = HubState::new(
            vec![SourceSeed {
                machine: "a".into(),
                health: Health::Reporting {
                    observed_at_unix_ms: 1,
                },
                snapshot: Some(seed.clone()),
            }],
            1,
        );
        hub.accept_snapshot("a", seed, 2);
        assert_eq!(hub.current().revision, 2);
        hub.mark_not_reached("a", 3);
        let stale = hub.current();
        assert_eq!(stale.revision, 3);
        assert_eq!(stale.agents.len(), 1);
        assert_eq!(stale.agents[0].activity_state(), "needs_attention");
        assert_eq!(
            stale.sources[0].health,
            Health::NotReached { since_unix_ms: 3 }
        );
        hub.mark_not_reached("a", 4);
        assert_eq!(hub.current().revision, 3);
        hub.accept_snapshot("a", snapshot("i", 7, "active"), 5);
        hub.mark_not_reached("a", 6);
        assert_eq!(
            hub.current().sources[0].health,
            Health::NotReached { since_unix_ms: 6 }
        );
    }

    #[tokio::test]
    async fn subscriber_slot_coalesces_to_complete_current_snapshot() {
        let hub = HubState::new(
            vec![SourceSeed {
                machine: "a".into(),
                health: Health::NotReached { since_unix_ms: 1 },
                snapshot: None,
            }],
            1,
        );
        let mut receiver = hub.subscribe();
        assert_eq!(receiver.borrow_and_update().revision, 1);
        hub.accept_snapshot("a", snapshot("i", 7, "active"), 2);
        hub.accept_snapshot("a", snapshot("i", 8, "active"), 3);
        hub.accept_snapshot("a", snapshot("i", 9, "active"), 4);
        receiver.changed().await.unwrap();
        assert_eq!(receiver.borrow_and_update().revision, 4);
        assert_eq!(receiver.borrow().agents[0].id.pid, 9);
    }

    #[test]
    fn no_agentd_clears_prior_agents_without_leaking_unprojected_values() {
        let hub = HubState::new(
            vec![SourceSeed {
                machine: "a".into(),
                health: Health::Reporting {
                    observed_at_unix_ms: 1,
                },
                snapshot: Some(snapshot("i", 7, "active")),
            }],
            1,
        );
        hub.mark_no_agentd("a", 2);
        let current = hub.current();
        assert!(current.agents.is_empty());
        assert_eq!(current.sources[0].instance_id, None);
        assert_eq!(
            serde_json::to_value(&*current).unwrap()["sources"][0]["scan"],
            Value::Null
        );
    }
}
