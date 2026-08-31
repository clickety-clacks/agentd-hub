use crate::state::HubState;
use axum::Json;
use axum::Router;
use axum::extract::State;
use axum::http::{Method, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::sse::{Event, Sse};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{MethodFilter, on};
use std::convert::Infallible;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::WatchStream;

pub fn router(hub: HubState) -> Router {
    Router::new()
        .route("/snapshot", on(MethodFilter::GET, snapshot))
        .route("/events", on(MethodFilter::GET, events))
        .route("/", on(MethodFilter::GET, root))
        .fallback(reject_route)
        .with_state(hub)
        .layer(middleware::from_fn(only_get))
}

async fn only_get(request: axum::extract::Request, next: Next) -> Response {
    if request.method() != Method::GET {
        return StatusCode::METHOD_NOT_ALLOWED.into_response();
    }
    next.run(request).await
}

async fn snapshot(State(hub): State<HubState>) -> Json<crate::model::HubSnapshot> {
    Json((*hub.current()).clone())
}

async fn events(
    State(hub): State<HubState>,
) -> Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>> {
    let stream = WatchStream::new(hub.subscribe()).map(|snapshot| {
        let data = serde_json::to_string(&*snapshot).expect("hub snapshots serialize");
        Ok(Event::default()
            .event("snapshot")
            .id(snapshot.revision.to_string())
            .data(data))
    });
    Sse::new(stream)
}

async fn root() -> Html<&'static str> {
    Html(INDEX_HTML)
}

async fn reject_route(method: Method) -> Response {
    let status = if matches!(method, Method::GET | Method::HEAD) {
        StatusCode::NOT_FOUND
    } else {
        StatusCode::METHOD_NOT_ALLOWED
    };
    (
        status,
        [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
        status.as_str().to_owned(),
    )
        .into_response()
}

pub const INDEX_HTML: &str = r#"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Agentd hub</title>
<style>
:root{color-scheme:light dark;font-family:ui-monospace,monospace}body{max-width:90rem;margin:2rem auto;padding:0 1rem}h1{font:inherit;font-weight:700}.source{border:1px solid #777;border-radius:.4rem;margin:1rem 0;padding:1rem}.health{opacity:.8}.agent{display:grid;grid-template-columns:repeat(auto-fit,minmax(12rem,1fr));gap:.35rem;border-top:1px solid #777;padding:.7rem 0}.attention{font-weight:700}.label{opacity:.7}
</style>
</head>
<body>
<h1>Agentd hub</h1>
<p id="status">Connecting…</p>
<main id="roster"></main>
<script>
'use strict';
let current=null;
const statusNode=document.getElementById('status');
const roster=document.getElementById('roster');
function text(tag,value,className){const element=document.createElement(tag);if(className)element.className=className;element.textContent=String(value);return element}
function valueOrUnknown(value){return value===null||value===undefined||value===''?'unknown':value}
function claimAge(activity){const stamp=activity&&activity.observedAtUnixMs;if(!Number.isSafeInteger(stamp)||stamp>Date.now())return'unknown';return Math.floor((Date.now()-stamp)/1000)+'s'}
function objectText(value){return value===null||value===undefined?'unknown':JSON.stringify(value)}
function healthText(health){if(health.state==='not_reached')return'not_reached since '+new Date(health.sinceUnixMs).toISOString();if(health.state==='reporting')return'reporting at '+new Date(health.observedAtUnixMs).toISOString();if(health.state==='no_agentd')return'no_agentd at '+new Date(health.observedAtUnixMs).toISOString();return String(health.state)}
function field(label,value){const cell=document.createElement('div');cell.append(text('span',label+': ','label'),text('span',value));return cell}
function render(){if(!current)return;roster.replaceChildren();for(const source of current.sources){const section=document.createElement('section');section.className='source';section.append(text('h2',source.machine),text('p',healthText(source.health),'health'));const agents=current.agents.filter(agent=>agent.machine===source.machine).sort((left,right)=>{const attention=Number(right.activity.state==='needs_attention')-Number(left.activity.state==='needs_attention');if(attention)return attention;return String(valueOrUnknown(left.name)).localeCompare(String(valueOrUnknown(right.name)))||String(left.harness).localeCompare(String(right.harness))||left.id.pid-right.id.pid||left.id.startTimeTicks-right.id.startTimeTicks});for(const agent of agents){const row=document.createElement('article');row.className=agent.activity.state==='needs_attention'?'agent attention':'agent';row.append(field('activity',agent.activity.state),field('claim age',claimAge(agent.activity)),field('name',valueOrUnknown(agent.name)),field('harness',agent.harness),field('pid',agent.id.pid),field('cwd',valueOrUnknown(agent.cwd.value)),field('tty',valueOrUnknown(agent.tty)),field('tmux',objectText(agent.tmux)),field('presence',objectText(agent.presence)));section.append(row)}roster.append(section)}statusNode.textContent='revision '+current.revision}
const events=new EventSource('/events');
events.addEventListener('snapshot',event=>{current=JSON.parse(event.data);render()});
events.addEventListener('error',()=>{statusNode.textContent='Disconnected; reconnecting…'});
setInterval(render,1000);
</script>
</body>
</html>
"#;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Health, parse_agentd_snapshot};
    use crate::state::SourceSeed;
    use axum::body::Body;
    use axum::http::Request;
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    fn hub() -> HubState {
        HubState::new(
            vec![SourceSeed {
                machine: "markup-<script>external.example".into(),
                health: Health::NotReached { since_unix_ms: 1 },
                snapshot: None,
            }],
            1,
        )
    }

    #[tokio::test]
    async fn snapshot_root_and_strict_routes_have_exact_media_types() {
        let app = router(hub());
        let snapshot = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/snapshot")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(snapshot.status(), StatusCode::OK);
        assert_eq!(snapshot.headers()[header::CONTENT_TYPE], "application/json");
        let root = app
            .clone()
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(
            root.headers()[header::CONTENT_TYPE],
            "text/html; charset=utf-8"
        );
        let unknown = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/unknown")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unknown.status(), StatusCode::NOT_FOUND);
        let post = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/snapshot")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(!post.status().is_success());
        let head = app
            .oneshot(
                Request::builder()
                    .method(Method::HEAD)
                    .uri("/snapshot")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(head.status(), StatusCode::METHOD_NOT_ALLOWED);
    }

    #[tokio::test]
    async fn first_sse_event_is_the_same_complete_snapshot() {
        let hub = hub();
        let expected = serde_json::to_string(&*hub.current()).unwrap();
        let response = router(hub.clone())
            .oneshot(
                Request::builder()
                    .uri("/events")
                    .header("last-event-id", "999")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.headers()[header::CONTENT_TYPE],
            "text/event-stream"
        );
        let mut body = response.into_body();
        let frame = body.frame().await.unwrap().unwrap();
        let text = std::str::from_utf8(frame.data_ref().unwrap()).unwrap();
        assert!(text.contains("event: snapshot"));
        assert!(text.contains("id: 1"));
        assert!(text.contains(&format!("data: {expected}")));

        let changed = parse_agentd_snapshot(
            br#"{"type":"snapshot","schema":"agentd.snapshot.v1","instanceId":"new-instance","revision":2,"observedAtUnixMs":2,"scan":{},"agents":[]}"#,
        )
        .unwrap();
        hub.accept_snapshot("markup-<script>external.example", changed, 2);
        let frame = body.frame().await.unwrap().unwrap();
        let text = std::str::from_utf8(frame.data_ref().unwrap()).unwrap();
        assert!(text.contains("id: 2"));
        assert!(text.contains("\"revision\":2"));
        assert!(text.contains("\"instanceId\":\"new-instance\""));
    }

    #[test]
    fn static_page_uses_only_text_dom_and_fixed_event_source() {
        assert!(INDEX_HTML.contains("new EventSource('/events')"));
        assert!(INDEX_HTML.contains("textContent"));
        assert!(INDEX_HTML.contains("createElement"));
        assert!(!INDEX_HTML.contains("innerHTML"));
        assert!(!INDEX_HTML.contains("fetch("));
        assert!(INDEX_HTML.contains("setInterval(render,1000)"));
        assert!(INDEX_HTML.contains("needs_attention"));
        assert!(INDEX_HTML.contains("claim age"));
        assert!(INDEX_HTML.contains("tmux"));
    }
}
