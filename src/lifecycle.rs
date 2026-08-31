use tokio::sync::watch;

// Shutdown is one monotonic process event: false becomes true once, and losing every
// sender also releases receivers so no child-owning phase can wait forever.
pub fn shutdown_channel() -> (watch::Sender<bool>, watch::Receiver<bool>) {
    watch::channel(false)
}

pub async fn shutdown_requested(shutdown: &mut watch::Receiver<bool>) {
    if *shutdown.borrow() {
        return;
    }
    let _ = shutdown.changed().await;
}
