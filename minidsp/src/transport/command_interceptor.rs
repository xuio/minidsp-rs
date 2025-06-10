use std::{
    pin::Pin,
    task::{Context, Poll},
};

use bytes::Bytes;
use futures::{Sink, Stream};
use pin_project::pin_project;
use tokio::sync::broadcast;

use minidsp_protocol::{commands::Commands, packet};
use crate::model::StatusSummary;

/// Creates a command interceptor that wraps a transport and broadcasts command events
pub fn command_interceptor<T>(
    inner: T,
    device_url: String,
    command_sender: broadcast::Sender<(String, StatusSummary)>,
) -> CommandInterceptor<T> {
    CommandInterceptor {
        inner,
        device_url,
        command_sender,
    }
}

/// Wraps a transport and intercepts commands to broadcast events
#[pin_project]
pub struct CommandInterceptor<T> {
    #[pin]
    inner: T,
    device_url: String,
    command_sender: broadcast::Sender<(String, StatusSummary)>,
}

impl<T> CommandInterceptor<T> {
    pub fn into_inner(self) -> T {
        self.inner
    }
}

impl<T, TErr> Stream for CommandInterceptor<T>
where
    T: Stream<Item = Result<Bytes, TErr>>,
{
    type Item = Result<Bytes, TErr>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        this.inner.poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<T> Sink<Bytes> for CommandInterceptor<T>
where
    T: Sink<Bytes>,
{
    type Error = T::Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().inner.poll_ready(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: Bytes) -> Result<(), Self::Error> {
        let this = self.project();
        
        // Intercept and parse the command
        if let Ok(frame) = packet::unframe(item.clone()) {
            if let Ok(command) = Commands::from_bytes(frame) {
                if let Some(state_change) = simulate_command_state_change(&command) {
                    let event = (this.device_url.clone(), state_change);
                    
                    // Best effort broadcast - ignore if no receivers
                    let _ = this.command_sender.send(event);
                }
            }
        }
        
        this.inner.start_send(item)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().inner.poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().inner.poll_close(cx)
    }
}

/// Simulate the state change that would result from a command
fn simulate_command_state_change(cmd: &Commands) -> Option<StatusSummary> {
    match cmd {
        Commands::SetVolume { value } => {
            Some(StatusSummary {
                master: crate::model::MasterStatus {
                    volume: Some(*value),
                    ..Default::default()
                },
                ..Default::default()
            })
        }
        Commands::SetMute { value } => {
            Some(StatusSummary {
                master: crate::model::MasterStatus {
                    mute: Some(*value),
                    ..Default::default()
                },
                ..Default::default()
            })
        }
        Commands::SetSource { source: _ } => {
            // We don't have device info here to convert source ID, but we can still indicate a change
            Some(StatusSummary {
                master: crate::model::MasterStatus {
                    // Could store raw source ID if we extended the model
                    ..Default::default()
                },
                ..Default::default()
            })
        }
        Commands::SetConfig { config, .. } => {
            Some(StatusSummary {
                master: crate::model::MasterStatus {
                    preset: Some(*config),
                    ..Default::default()
                },
                ..Default::default()
            })
        }
        Commands::DiracBypass { value } => {
            Some(StatusSummary {
                master: crate::model::MasterStatus {
                    dirac: Some(*value == 0), // 0 = enabled, 1 = disabled
                    ..Default::default()
                },
                ..Default::default()
            })
        }
        // Handle Unknown commands that might be DiracBypass (0x3f) for backward compatibility
        Commands::Unknown { cmd_id: 0x3f, payload } => {
            if payload.len() == 1 {
                let value = payload[0];
                Some(StatusSummary {
                    master: crate::model::MasterStatus {
                        dirac: Some(value == 0), // 0 = enabled, 1 = disabled
                        ..Default::default()
                    },
                    ..Default::default()
                })
            } else {
                None
            }
        }
        // Complex or read-only commands don't trigger state changes
        Commands::WriteBiquad { .. } => None,
        Commands::WriteBiquadBypass { .. } => None,
        Commands::Write { .. } => None,
        Commands::WriteMemory { .. } => None,
        Commands::FirLoadStart { .. } => None,
        Commands::FirLoadData { .. } => None,
        Commands::FirLoadEnd => None,
        Commands::SwitchMux { .. } => None,
        Commands::BulkLoad { .. } => None,
        Commands::BulkLoadFilterData { .. } => None,
        Commands::Unk07 { .. } => None,
        Commands::Unknown { .. } => None,
        Commands::ReadHardwareId => None,
        Commands::ReadFloats { .. } => None,
        Commands::ReadMemory { .. } => None,
        Commands::Read { .. } => None,
    }
} 