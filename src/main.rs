use agentd_hub::discovery::{Deadlines, Programs, discover, unix_time_ms};
use agentd_hub::state::HubState;
use agentd_hub::supervisor::spawn_watchers;
use agentd_hub::web;
use std::net::SocketAddr;
use std::path::PathBuf;

const USAGE: &str = "usage: agentd-hub [--listen LOOPBACK:PORT] [--hosts-file PATH]";

struct Options {
    listen: SocketAddr,
    hosts_file: Option<PathBuf>,
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("agentd-hub: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    let Some(options) = parse_options()? else {
        return Ok(());
    };
    if !options.listen.ip().is_loopback() {
        return Err(format!(
            "non_loopback_listen_refused: {} is not a loopback address",
            options.listen.ip()
        ));
    }

    let programs = Programs::default();
    let deadlines = Deadlines::default();
    let seeds = discover(&programs, deadlines, options.hosts_file.as_deref())
        .await
        .map_err(|error| error.to_string())?;
    let hub = HubState::new(seeds, unix_time_ms());
    let listener = tokio::net::TcpListener::bind(options.listen)
        .await
        .map_err(|error| format!("listen_failed: {error}"))?;
    #[cfg(unix)]
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .map_err(|error| format!("signal_handler_failed: {error}"))?;
    let app = web::router(hub.clone());
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let watchers = spawn_watchers(hub, programs, deadlines, shutdown_rx);
    let result = {
        let server = async move {
            axum::serve(listener, app)
                .await
                .map_err(|error| format!("server_failed: {error}"))
        };
        tokio::pin!(server);
        #[cfg(unix)]
        let result = tokio::select! {
            result = &mut server => result,
            _ = tokio::signal::ctrl_c() => Ok(()),
            _ = terminate.recv() => Ok(()),
        };
        #[cfg(not(unix))]
        let result = tokio::select! {
            result = &mut server => result,
            _ = tokio::signal::ctrl_c() => Ok(()),
        };
        result
    };
    shutdown_tx.send_replace(true);
    let mut watcher_error = None;
    for watcher in watchers {
        if let Err(error) = watcher.await {
            watcher_error.get_or_insert_with(|| format!("watcher_task_failed: {error}"));
        }
    }
    if let Some(error) = watcher_error {
        return Err(error);
    }
    result
}

fn parse_options() -> Result<Option<Options>, String> {
    parse_options_from(std::env::args().skip(1))
}

fn parse_options_from(
    arguments: impl IntoIterator<Item = String>,
) -> Result<Option<Options>, String> {
    let mut listen = "127.0.0.1:8787".parse().expect("valid default address");
    let mut hosts_file = None;
    let mut args = arguments.into_iter();
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--listen" => {
                let value = args.next().ok_or_else(|| USAGE.to_owned())?;
                listen = value
                    .parse()
                    .map_err(|_| format!("invalid_listen_address: {value}"))?;
            }
            "--hosts-file" => {
                hosts_file = Some(PathBuf::from(args.next().ok_or_else(|| USAGE.to_owned())?));
            }
            "--help" | "-h" => {
                println!("{USAGE}");
                return Ok(None);
            }
            "--version" | "-V" => {
                println!("agentd-hub {}", env!("CARGO_PKG_VERSION"));
                return Ok(None);
            }
            _ => return Err(format!("unknown_argument: {argument}\n{USAGE}")),
        }
    }
    Ok(Some(Options { listen, hosts_file }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_listener_is_ipv4_loopback_port_8787() {
        let options = parse_options_from(Vec::new()).unwrap().unwrap();
        assert_eq!(options.listen, "127.0.0.1:8787".parse().unwrap());
        assert!(options.listen.ip().is_loopback());
    }

    #[test]
    fn listen_option_accepts_ipv6_loopback_and_hosts_file() {
        let options = parse_options_from([
            "--listen".into(),
            "[::1]:9000".into(),
            "--hosts-file".into(),
            "hosts".into(),
        ])
        .unwrap()
        .unwrap();
        assert!(options.listen.ip().is_loopback());
        assert_eq!(options.hosts_file, Some(PathBuf::from("hosts")));
    }
}
