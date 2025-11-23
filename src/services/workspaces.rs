use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    rc::Rc,
    sync::Arc,
    thread::{self, JoinHandle},
};

use log::{debug, error, info, trace, warn};
use wayland_client::{
    Connection, Dispatch, Proxy, QueueHandle, WEnum,
    protocol::{
        wl_display::WlDisplay,
        wl_output::WlOutput,
        wl_registry::{self, WlRegistry},
    },
};
use wayland_protocols::ext::workspace::v1::client::{
    ext_workspace_group_handle_v1::{
        Event as WorkspaceGroupEvent, ExtWorkspaceGroupHandleV1, GroupCapabilities,
    },
    ext_workspace_handle_v1::{
        Event as WorkspaceEvent, ExtWorkspaceHandleV1, State as WorkspaceState,
        WorkspaceCapabilities,
    },
    ext_workspace_manager_v1::{
        EVT_WORKSPACE_GROUP_OPCODE, EVT_WORKSPACE_OPCODE, Event as WorkspaceManagerEvent,
        ExtWorkspaceManagerV1,
    },
};

#[derive(Default, Debug, PartialEq, Eq, Hash)]
pub struct Workspace {
    pub id: Option<String>,
    pub name: Option<String>,
    pub coordinates: Vec<u8>,
    pub state: Option<WorkspaceState>,
    pub capabilities: Option<WorkspaceCapabilities>,
}
type WorkspaceRef = Rc<RefCell<Workspace>>;

#[derive(Default, Debug)]
pub struct WorkspaceGroup {
    pub capabilities: Option<GroupCapabilities>,
    pub outputs: Vec<OutputRef>,
    pub workspaces: Vec<WorkspaceRef>,
}

#[derive(Debug, Default, PartialEq, Eq, Hash)]
pub struct Output {
    pub name: Option<String>,
    pub description: Option<String>,
}
type OutputRef = Rc<RefCell<Output>>;

#[derive(Default)]
struct WorkspaceProtocolData {
    _manager: Option<(ExtWorkspaceManagerV1, u32)>,
    workspaces: HashMap<ExtWorkspaceHandleV1, WorkspaceRef>,
    groups: HashMap<ExtWorkspaceGroupHandleV1, WorkspaceGroup>,
    outputs: HashMap<WlOutput, OutputRef>,
}

pub struct WorkspaceProtocol {
    _connection: Connection,
    _display: WlDisplay,
    _registry: WlRegistry,
    _event_thread: JoinHandle<anyhow::Result<()>>,
    _handle: QueueHandle<WorkspaceProtocolData>,
}

impl WorkspaceProtocol {
    pub fn new() -> Option<Self> {
        let init = || -> anyhow::Result<Self> {
            let connection = Connection::connect_to_env()?;
            let display = connection.display();
            let mut event_queue = connection.new_event_queue();
            let handle = event_queue.handle();
            let registry = display.get_registry(&handle, UserData {});
            let event_thread = thread::spawn(move || {
                let mut data = WorkspaceProtocolData::default();
                event_queue.roundtrip(&mut data)?;
                loop {
                    event_queue.blocking_dispatch(&mut data)?;
                }
            });

            let obj = Self {
                _connection: connection,
                _display: display,
                _registry: registry,
                _event_thread: event_thread,
                _handle: handle,
            };

            Ok(obj)
        };

        match init() {
            Ok(obj) => Some(obj),
            Err(err) => {
                error!("Failed to initialize workspace protocol manager: {err}");
                None
            }
        }
    }
}

struct UserData;

impl Dispatch<WlRegistry, UserData, Self> for WorkspaceProtocolData {
    fn event(
        state: &mut Self,
        proxy: &WlRegistry,
        event: wl_registry::Event,
        _data: &UserData,
        _conn: &Connection,
        qh: &QueueHandle<Self>,
    ) {
        trace!("WlRegistry event: {:?}", event);
        match event {
            wl_registry::Event::Global {
                name,
                interface,
                version,
            } => {
                if interface == ExtWorkspaceManagerV1::interface().name {
                    debug!("Registered ExtWorkspaceManagerV1 v{version} (name: {name})");
                    let manager = proxy.bind(name, version, qh, UserData);
                    if state._manager.is_some() {
                        error!("ExtWorkspaceManagerV1 is already bound!");
                    } else {
                        state._manager = Some((manager, name));
                        info!("Bound to ExtWorkspaceManagerV1");
                    }
                } else if interface == WlOutput::interface().name {
                    debug!("Registered WlOutput v{version} (name: {name})");
                    let output = proxy.bind(name, version, qh, UserData {});
                    state
                        .outputs
                        .insert(output, RefCell::new(Output::default()).into());
                    info!("Bound to WlOutput");
                }
            }
            wl_registry::Event::GlobalRemove { name } => {
                if let Some((_, mgr_name)) = &state._manager {
                    if *mgr_name == name {
                        state._manager = None;
                        warn!("ExtWorkspaceManagerV1 has been removed");
                    }
                }
            }
            _ => {}
        }
    }
}

impl Dispatch<ExtWorkspaceManagerV1, UserData, WorkspaceProtocolData> for WorkspaceProtocolData {
    fn event(
        state: &mut Self,
        _proxy: &ExtWorkspaceManagerV1,
        event: WorkspaceManagerEvent,
        _data: &UserData,
        _conn: &Connection,
        _qh: &QueueHandle<Self>,
    ) {
        trace!("ExtWorkspaceManagerV1 event: {:?}", event);
        match event {
            WorkspaceManagerEvent::WorkspaceGroup { workspace_group } => {
                state
                    .groups
                    .insert(workspace_group, WorkspaceGroup::default());
            }
            WorkspaceManagerEvent::Workspace { workspace } => {
                state
                    .workspaces
                    .insert(workspace, RefCell::new(Workspace::default()).into());
            }
            WorkspaceManagerEvent::Done => {
                info!("Finished receiving workspace data");
                debug!("Workspaces: {:#?}", state.workspaces);
                debug!("Workspace Groups: {:#?}", state.groups);
            }
            WorkspaceManagerEvent::Finished => {
                warn!("Workspace manager has finished and will no longer send events");
            }
            _ => {
                error!("Unknown ExtWorkspaceManagerV1 event: {:?}", event);
            }
        }
    }

    fn event_created_child(
        opcode: u16,
        handle: &QueueHandle<Self>,
    ) -> Arc<dyn wayland_client::backend::ObjectData> {
        log::trace!("Created child object with opcode: {}", opcode);
        match opcode {
            EVT_WORKSPACE_GROUP_OPCODE => {
                handle.make_data::<ExtWorkspaceGroupHandleV1, _>(UserData {})
            }
            EVT_WORKSPACE_OPCODE => handle.make_data::<ExtWorkspaceHandleV1, _>(UserData {}),
            _ => panic!("Unknown opcode for child object: {}", opcode),
        }
    }
}

impl Dispatch<ExtWorkspaceHandleV1, UserData, WorkspaceProtocolData> for WorkspaceProtocolData {
    fn event(
        state: &mut Self,
        proxy: &ExtWorkspaceHandleV1,
        event: WorkspaceEvent,
        _data: &UserData,
        _conn: &Connection,
        _qh: &QueueHandle<Self>,
    ) {
        trace!("ExtWorkspaceHandleV1 event: {:?}", event);
        let is_removed = matches!(event, WorkspaceEvent::Removed);
        {
            let Some(workspace) = state.workspaces.get_mut(proxy) else {
                error!("Received event for unknown workspace: {:?}", event);
                return;
            };
            let workspace = &mut workspace.borrow_mut();
            match event {
                WorkspaceEvent::Id { id } => {
                    workspace.id = Some(id);
                }
                WorkspaceEvent::Name { name } => {
                    workspace.name = Some(name);
                }
                WorkspaceEvent::Coordinates { coordinates } => {
                    workspace.coordinates = coordinates;
                }
                WorkspaceEvent::State { state } => {
                    workspace.state = match state {
                        WEnum::Value(state) => {
                            debug!("Workspace state updated: {:?}", state);
                            Some(state)
                        }
                        WEnum::Unknown(value) => {
                            warn!("Received unknown workspace state value: {}", value);
                            None
                        }
                    };
                }
                WorkspaceEvent::Capabilities { capabilities } => {
                    workspace.capabilities = match capabilities {
                        WEnum::Value(capabilities) => {
                            debug!("Workspace capabilities updated: {:?}", capabilities);
                            Some(capabilities)
                        }
                        WEnum::Unknown(value) => {
                            warn!("Received unknown workspace capabilities value: {}", value);
                            None
                        }
                    };
                }
                WorkspaceEvent::Removed => {
                    debug!("Workspace removed: {:?}", workspace);
                }
                _ => {
                    error!("Unknown ExtWorkspaceHandleV1 event: {:?}", event);
                }
            }
        }
        if is_removed {
            // Can't remove inside the Removed match because workspace is still borrowed
            state.workspaces.remove(proxy);
        }
    }
}

impl Dispatch<ExtWorkspaceGroupHandleV1, UserData, WorkspaceProtocolData>
    for WorkspaceProtocolData
{
    fn event(
        state: &mut Self,
        proxy: &ExtWorkspaceGroupHandleV1,
        event: WorkspaceGroupEvent,
        _data: &UserData,
        _conn: &Connection,
        _qh: &QueueHandle<Self>,
    ) {
        trace!("ExtWorkspaceGroupHandleV1 event: {:?}", event);
        let Some(group) = state.groups.get_mut(proxy) else {
            error!("Received event for unknown workspace group: {:?}", event);
            return;
        };
        match event {
            WorkspaceGroupEvent::Capabilities { capabilities } => {
                group.capabilities = match capabilities {
                    WEnum::Value(capabilities) => {
                        debug!("Workspace group capabilities updated: {:?}", capabilities);
                        Some(capabilities)
                    }
                    WEnum::Unknown(value) => {
                        warn!(
                            "Received unknown workspace group capabilities value: {}",
                            value
                        );
                        None
                    }
                };
            }
            WorkspaceGroupEvent::OutputEnter { output } => {
                let Some(output) = state.outputs.get(&output) else {
                    warn!("Received OutputEnter for unknown output");
                    return;
                };
                group.outputs.push(output.clone());
            }
            WorkspaceGroupEvent::OutputLeave { output } => {
                let Some(output) = state.outputs.get(&output) else {
                    warn!("Received OutputLeave for unknown output");
                    return;
                };
                group.outputs.retain(|o| !Rc::ptr_eq(o, output));
            }
            WorkspaceGroupEvent::WorkspaceEnter { workspace } => {
                let Some(workspace) = state.workspaces.get(&workspace) else {
                    warn!("Received WorkspaceEnter for unknown workspace");
                    return;
                };
                group.workspaces.push(workspace.clone());
            }
            WorkspaceGroupEvent::WorkspaceLeave { workspace } => {
                let Some(workspace) = state.workspaces.get(&workspace) else {
                    warn!("Received WorkspaceLeave for unknown workspace");
                    return;
                };
                group.workspaces.retain(|w| !Rc::ptr_eq(w, workspace));
            }
            WorkspaceGroupEvent::Removed => {
                debug!("Workspace group removed: {group:?}",);
                state.groups.remove(proxy);
            }
            _ => {
                error!("Unknown ExtWorkspaceGroupHandleV1 event: {:?}", event);
            }
        }
    }
}

impl Dispatch<WlOutput, UserData, WorkspaceProtocolData> for WorkspaceProtocolData {
    fn event(
        state: &mut Self,
        proxy: &WlOutput,
        event: wayland_client::protocol::wl_output::Event,
        _data: &UserData,
        _conn: &Connection,
        _qh: &QueueHandle<Self>,
    ) {
        trace!("WlOutput event: {:?}", event);
        let Some(output) = state.outputs.get_mut(proxy) else {
            error!("Received event for unknown output: {:?}", event);
            return;
        };
        let mut output = output.borrow_mut();
        match event {
            wayland_client::protocol::wl_output::Event::Name { name } => {
                output.name = Some(name);
            }
            wayland_client::protocol::wl_output::Event::Description { description } => {
                output.description = Some(description);
            }
            _ => {
                // Ignore other events for now
            }
        }
    }
}
