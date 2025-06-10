use tokio::sync::broadcast;
use minidsp::model::StatusSummary;

/// Simplified command event - just device URL and status update
pub type CommandEvent = (String, StatusSummary);

/// Create a new command event broadcaster - just returns the channel
pub fn create_command_broadcaster(capacity: usize) -> broadcast::Sender<CommandEvent> {
    let (sender, _) = broadcast::channel(capacity);
    sender
}

/// Subscribe to command events for a specific device
pub fn subscribe_device_commands(
    rx: broadcast::Receiver<CommandEvent>, 
    device_url: String
) -> impl futures::Stream<Item = StatusSummary> {
    futures::stream::unfold(rx, move |mut rx| {
        let device_url = device_url.clone();
        async move {
            loop {
                match rx.recv().await {
                    Ok((url, status)) => {
                        if url == device_url {
                            return Some((status, rx));
                        }
                    }
                    Err(_) => return None,
                }
            }
        }
    })
} 