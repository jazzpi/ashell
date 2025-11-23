use std::{any::TypeId, collections::HashSet, rc::Rc, sync::Arc};

use iced::{
    Subscription,
    futures::{SinkExt, channel::mpsc::Sender},
    stream::channel,
};
use log::debug;

use crate::services::{ReadOnlyService, ServiceEvent};

mod wayland;

pub use wayland::{Output, Workspace};

#[derive(Debug, Default, Clone)]
pub struct WorkspaceGroup {
    pub outputs: HashSet<Arc<Output>>,
    pub workspaces: HashSet<Arc<Workspace>>,
}

#[derive(Debug, Default, Clone)]
pub struct WorkspaceSnapshot {
    pub workspaces: Vec<Arc<Workspace>>,
    pub groups: Vec<Arc<WorkspaceGroup>>,
    pub outputs: Vec<Arc<Output>>,
}

#[derive(Debug, Default, Clone)]
pub struct WorkspacesService {
    data: WorkspaceSnapshot,
}

enum State {
    Init,
    Active(wayland::WorkspaceProtocol),
    Error,
}

impl WorkspacesService {
    async fn start_listening(state: State, output: &mut Sender<ServiceEvent<Self>>) -> State {
        match state {
            State::Init => match wayland::WorkspaceProtocol::new() {
                Ok(protocol) => {
                    output
                        .send(ServiceEvent::Init(WorkspacesService::default()))
                        .await
                        .ok();
                    State::Active(protocol)
                }
                Err(_) => State::Error,
            },
            State::Active(mut protocol) => match protocol.next_event().await {
                Ok(event) => {
                    output.send(ServiceEvent::Update(event)).await.ok();
                    State::Active(protocol)
                }
                Err(_) => State::Error,
            },
            State::Error => {
                // In case of error, we wait a bit before trying to re-initialize
                iced::futures::future::ready(()).await;
                State::Init
            }
        }
    }
}

impl ReadOnlyService for WorkspacesService {
    type UpdateEvent = WorkspaceSnapshot;
    type Error = ();

    fn update(&mut self, event: Self::UpdateEvent) {
        self.data = event;
    }

    // TODO: Would be nice if we could return Option<Subscription<...>> here
    fn subscribe() -> Subscription<ServiceEvent<Self>> {
        let id = TypeId::of::<Self>();

        Subscription::run_with_id(
            id,
            channel(100, async |mut output| {
                let mut state = State::Init;
                loop {
                    state = Self::start_listening(state, &mut output).await;
                }
            }),
        )
    }
}
