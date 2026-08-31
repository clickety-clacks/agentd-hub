use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentdSnapshot {
    #[serde(rename = "type")]
    pub frame_type: String,
    pub schema: String,
    pub instance_id: String,
    pub revision: u64,
    pub observed_at_unix_ms: u64,
    pub scan: Value,
    pub agents: Vec<AgentdAgent>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentdAgent {
    pub id: AgentId,
    pub harness: Harness,
    pub detected_by: DetectedBy,
    pub presence: Presence,
    pub cwd: Cwd,
    pub activity: Activity,
    #[serde(default)]
    pub tty: Option<String>,
    #[serde(default)]
    pub tmux: Option<TmuxLocation>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub started_at_unix_ms: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentId {
    pub pid: u32,
    pub start_time_ticks: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Harness {
    Codex,
    Claude,
}

impl Harness {
    fn as_str(self) -> &'static str {
        match self {
            Self::Codex => "codex",
            Self::Claude => "claude",
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DetectedBy {
    ProcComm,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IssueCause {
    PermissionDenied,
    ProcessRaced,
    IoError,
    ProcUnavailable,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum PresenceState {
    Present,
    Unknown,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Presence {
    pub state: PresenceState,
    pub cause: Option<IssueCause>,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum CwdState {
    Known,
    Unknown,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Cwd {
    pub state: CwdState,
    pub value: Option<String>,
    pub cause: Option<IssueCause>,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ActivityState {
    Active,
    Idle,
    NeedsAttention,
    Unknown,
}

impl ActivityState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Idle => "idle",
            Self::NeedsAttention => "needs_attention",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ActivitySource {
    Hook,
    None,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Activity {
    pub state: ActivityState,
    pub source: ActivitySource,
    pub observed_at_unix_ms: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TmuxLocation {
    pub session: String,
    pub window_index: u32,
    pub window_name: String,
    pub pane_id: String,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HubAgent {
    pub machine: String,
    pub instance_id: String,
    pub id: AgentId,
    pub harness: Harness,
    pub detected_by: DetectedBy,
    pub presence: Presence,
    pub cwd: Cwd,
    pub activity: Activity,
    pub tty: Option<String>,
    pub tmux: Option<TmuxLocation>,
    pub name: Option<String>,
    pub started_at_unix_ms: Option<u64>,
}

impl HubAgent {
    pub fn activity_state(&self) -> &str {
        self.activity.state.as_str()
    }

    pub fn display_name(&self) -> &str {
        self.name.as_deref().unwrap_or("")
    }

    pub fn harness_name(&self) -> &str {
        self.harness.as_str()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum Health {
    Reporting {
        #[serde(rename = "observedAtUnixMs")]
        observed_at_unix_ms: u64,
    },
    NotReached {
        #[serde(rename = "sinceUnixMs")]
        since_unix_ms: u64,
    },
    NoAgentd {
        #[serde(rename = "observedAtUnixMs")]
        observed_at_unix_ms: u64,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceRow {
    pub machine: String,
    pub health: Health,
    pub instance_id: Option<String>,
    pub source_revision: Option<u64>,
    pub source_observed_at_unix_ms: Option<u64>,
    pub scan: Option<Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HubSnapshot {
    #[serde(rename = "type")]
    pub frame_type: &'static str,
    pub schema: &'static str,
    pub revision: u64,
    pub observed_at_unix_ms: u64,
    pub sources: Vec<SourceRow>,
    pub agents: Vec<HubAgent>,
}

pub fn parse_agentd_snapshot(bytes: &[u8]) -> Result<AgentdSnapshot, String> {
    let snapshot: AgentdSnapshot =
        serde_json::from_slice(bytes).map_err(|error| format!("invalid Agentd JSON: {error}"))?;
    snapshot.validate()?;
    Ok(snapshot)
}

impl AgentdSnapshot {
    fn validate(&self) -> Result<(), String> {
        if self.frame_type != "snapshot" {
            return Err("Agentd frame type is not snapshot".into());
        }
        if self.schema != "agentd.snapshot.v1" {
            return Err("Agentd frame schema is not agentd.snapshot.v1".into());
        }
        if self.instance_id.is_empty() {
            return Err("Agentd instanceId is empty".into());
        }
        if !self.scan.is_object() {
            return Err("Agentd scan is not an object".into());
        }
        Ok(())
    }
}

impl AgentdAgent {
    pub fn project(&self, machine: &str, instance_id: &str) -> HubAgent {
        HubAgent {
            machine: machine.to_owned(),
            instance_id: instance_id.to_owned(),
            id: self.id.clone(),
            harness: self.harness,
            detected_by: self.detected_by,
            presence: self.presence.clone(),
            cwd: self.cwd.clone(),
            activity: self.activity.clone(),
            tty: self.tty.clone(),
            tmux: self.tmux.clone(),
            name: self.name.clone(),
            started_at_unix_ms: self.started_at_unix_ms,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn validates_and_projects_only_named_agent_fields() {
        let input = json!({
            "type": "snapshot",
            "reason": "initial",
            "schema": "agentd.snapshot.v1",
            "instanceId": "instance-a",
            "revision": 4,
            "observedAtUnixMs": 9,
            "scan": {"state":"complete","issues":[]},
            "agents": [{
                "id":{"pid":7,"startTimeTicks":11},
                "harness":"codex",
                "detectedBy":"proc_comm",
                "presence":{"state":"present","cause":null},
                "cwd":{"state":"known","value":"/work","cause":null},
                "activity":{"state":"unknown","source":"none","observedAtUnixMs":null},
                "privateSentinel":"must-not-project"
            }],
            "futureTopLevel": "ignored"
        });
        let parsed = parse_agentd_snapshot(&serde_json::to_vec(&input).unwrap()).unwrap();
        let projected = parsed.agents[0].project("gibson", &parsed.instance_id);
        let bytes = serde_json::to_vec(&projected).unwrap();
        assert!(
            !bytes
                .windows(b"must-not-project".len())
                .any(|part| part == b"must-not-project")
        );
        assert_eq!(projected.tty, None);
        assert_eq!(projected.tmux, None);
        assert_eq!(projected.name, None);
        assert_eq!(projected.started_at_unix_ms, None);
    }

    #[test]
    fn rejects_invalid_required_agent_types() {
        let input = br#"{"type":"snapshot","schema":"agentd.snapshot.v1","instanceId":"i","revision":1,"observedAtUnixMs":1,"scan":{},"agents":[{"id":{"pid":7,"startTimeTicks":9},"harness":7,"detectedBy":"proc_comm","presence":{},"cwd":{},"activity":{}}]}"#;
        assert!(parse_agentd_snapshot(input).is_err());
    }
}
