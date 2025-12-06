use crate::fts::FtsEventPipe;
use crate::touch_emulator::TouchEmulator;
use crate::touch_merger::{TouchMerger, TouchSourceDeclaration};
use anyhow::Context;
use async_trait::async_trait;
use binder::unstable_api::AsNative;
use binder::{ExceptionCode, Status};
use nix::libc::MADV_HWPOISON;
use std::collections::BTreeMap;
use std::ops::{DerefMut, Range};
use tokio::sync::{Mutex, MutexGuard, RwLock};
use tokio_util::sync::CancellationToken;
use vendor_lineage_touchinjector::{
    aidl::vendor::lineage::touchinjector::{
        ITouchInjector::ITouchInjectorAsyncServer, ITouchInjectorClient::ITouchInjectorClient,
        TouchPosition::TouchPosition,
    },
    binder::{Interface, Strong},
};

const BASE_SLOT_COUNT: usize = 10;

type ClientPtr = Strong<dyn ITouchInjectorClient>;

pub struct TouchInjectorDeviceState {
    fts_worker: FtsEventPipe,
    emulator: TouchEmulator,

    merger_join_handle: tokio::task::JoinHandle<anyhow::Result<()>>,
    merger_cancellation_token: CancellationToken,
}

pub struct TouchInjectorService {
    slot_count: RwLock<usize>,

    slot_range_map: RwLock<BTreeMap<ClientPtr, Range<usize>>>,
    pressed_position_map: RwLock<BTreeMap<ClientPtr, Box<[Option<(i32, i32)>]>>>,

    device_state: Mutex<Option<TouchInjectorDeviceState>>,
}

impl TouchInjectorService {
    pub async fn new() -> anyhow::Result<Self> {
        Ok(Self {
            slot_count: RwLock::new(0),

            slot_range_map: RwLock::new(BTreeMap::new()),
            pressed_position_map: RwLock::new(BTreeMap::new()),
            device_state: Mutex::new(None),
        })
    }

    async fn create_device<'a>(
        &self,
        device_state_lock: &mut MutexGuard<'a, Option<TouchInjectorDeviceState>>,
    ) -> anyhow::Result<()> {
        log::debug!("Creating virtual touch device");

        let slot_count = *self.slot_count.read().await;
        log::debug!("Allocating {slot_count} additional multi-touch slots");

        let (mut emulator, touch_emulator_rx) =
            TouchEmulator::new(slot_count).context("Failed to create touch emulator")?;

        let mut fts_worker = FtsEventPipe::new().context("Failed to create fts worker")?;

        let (merger_cancellation_token, merger_join_handle) = {
            let merger = TouchMerger::new(Box::from([
                (
                    TouchSourceDeclaration::new(BASE_SLOT_COUNT),
                    fts_worker.take_input_event_receiver().unwrap(),
                ),
                (TouchSourceDeclaration::new(slot_count), touch_emulator_rx),
            ]))
            .context("Failed to create touch merger")?;

            let cancellation_token = CancellationToken::new();
            let child_cancellation_token = cancellation_token.child_token();

            let join_handle = tokio::spawn(merger.processing_task(child_cancellation_token));

            (cancellation_token, join_handle)
        };

        let pressed_position_map_lock = self.pressed_position_map.read().await;
        let slot_range_map_lock = self.slot_range_map.read().await;

        // brink presses back
        for (
            client,
            Range::<usize> {
                start: start_slot_id,
                ..
            },
        ) in slot_range_map_lock.iter()
        {
            let Some(is_pressed_vec) = pressed_position_map_lock.get(client) else {
                continue;
            };

            for (index, position) in is_pressed_vec.iter().enumerate() {
                let Some(position) = position else {
                    continue;
                };

                let client_addr = client.as_binder().as_native() as usize;
                log::debug!("Restoring touch state for slot {index} owned by client {client_addr}");

                if let Err(err) = emulator
                    .begin_touch(start_slot_id + index, position.0, position.1)
                    .await
                {
                    log::warn!("Failed to begin touch!\n{err:?}");
                }
            }
        }

        let device_state = TouchInjectorDeviceState {
            fts_worker,
            emulator,
            merger_join_handle,
            merger_cancellation_token,
        };

        device_state_lock.replace(device_state);

        Ok(())
    }

    async fn destroy_device<'a>(
        &self,
        device_state_lock: &mut MutexGuard<'a, Option<TouchInjectorDeviceState>>,
    ) {
        log::debug!("Shutting down virtual touch device");

        let Some(TouchInjectorDeviceState {
            mut fts_worker,
            mut emulator,
            merger_join_handle,
            merger_cancellation_token,
        }) = device_state_lock.take()
        else {
            return;
        };

        emulator.end_all_touches().await;

        log::debug!("Waiting for FTS input redirector to stop");
        fts_worker.stop().await;
        log::debug!("FTS input redirector stopped");

        log::debug!("Waiting for input merger to stop");
        merger_cancellation_token.cancel();
        let _ = merger_join_handle.await;
        log::debug!("Input merger stopped");
    }

    fn invalid_argument_status(message: &str) -> Status {
        Status::new_exception_str(ExceptionCode::ILLEGAL_ARGUMENT, Some(message))
    }

    fn illegal_state_status(message: &str) -> Status {
        Status::new_exception_str(ExceptionCode::ILLEGAL_STATE, Some(message))
    }
}

impl Interface for TouchInjectorService {}

#[allow(non_snake_case)]
#[async_trait]
impl ITouchInjectorAsyncServer for TouchInjectorService {
    async fn r#registerClient<'a, 'l1>(
        &'a self,
        client: &'l1 Strong<dyn ITouchInjectorClient>,
    ) -> binder::Result<()> {
        let client_addr = client.as_binder().as_native() as usize;

        log::debug!("Registering client {client_addr}");

        if self.slot_range_map.read().await.contains_key(client) {
            log::warn!("Client {client_addr} is already registered",);
            return Ok(());
        }

        let client_slot_count = client.getSlotCount()? as usize;
        log::debug!("Client {client_addr} requested {client_slot_count} multi-touch slots",);

        if client_slot_count == 0 {
            log::warn!("Client {client_addr} requested zero multi-touch slots",);
            return Err(Self::invalid_argument_status(
                "Requested slot count must be greater than zero",
            ));
        }

        log::debug!("Locking device state for client registration");
        let mut device_state_lock = self.device_state.lock().await;

        self.destroy_device(&mut device_state_lock).await;

        let client_slots_from = *self.slot_count.read().await;
        let client_slots_to = client_slots_from + client_slot_count;

        *self.slot_count.write().await += client_slot_count;

        log::debug!(
            "Assigned slots {}..={} to client {client_addr}",
            BASE_SLOT_COUNT + client_slots_from,
            BASE_SLOT_COUNT + client_slots_to - 1
        );

        self.slot_range_map.write().await.insert(
            Strong::clone(client),
            Range::<usize>::from(client_slots_from..client_slots_to),
        );

        self.pressed_position_map
            .write()
            .await
            .insert(Strong::clone(client), {
                let mut slots = Vec::<Option<(i32, i32)>>::new();
                slots.resize(client_slot_count, None);
                slots.into_boxed_slice()
            });

        if let Err(err) = self.create_device(&mut device_state_lock).await {
            log::error!(
                "Failed to rebuild virtual device after registering client {client_addr}: {err:?}"
            );
            return Err(Self::illegal_state_status(
                "Unable to rebuild virtual touch device",
            ));
        }

        Ok(())
    }

    async fn r#unregisterClient<'a, 'l1>(
        &'a self,
        client: &'l1 Strong<dyn ITouchInjectorClient>,
    ) -> binder::Result<()> {
        let client_addr = client.as_binder().as_native() as usize;
        log::debug!("Unregistering client {client_addr}");

        if !self.slot_range_map.read().await.contains_key(client) {
            log::warn!("Client {client_addr} is not registered",);
            return Ok(());
        }

        log::debug!("Locking device state for client unregistration");
        let mut device_state_lock = self.device_state.lock().await;

        self.destroy_device(&mut device_state_lock).await;

        let client_slot_count = self.slot_range_map.read().await.get(client).unwrap().len();

        *self.slot_count.write().await -= client_slot_count;
        self.slot_range_map.write().await.remove(client);
        self.pressed_position_map.write().await.remove(client);

        // remap ranges (situations, when removed not last client).
        let mut end_slot_index: usize = 0;

        for range in self.slot_range_map.write().await.values_mut() {
            let client_slot_count = range.len();

            *range = Range::from(end_slot_index..(end_slot_index + client_slot_count));
            end_slot_index += client_slot_count;
        }

        if let Err(err) = self.create_device(&mut device_state_lock).await {
            log::error!(
                "Failed to rebuild virtual device after unregistering client {client_addr}: {err:?}"
            );
            return Err(Self::illegal_state_status(
                "Unable to rebuild virtual touch device",
            ));
        }

        Ok(())
    }

    async fn r#beginTouch<'a, 'l1, 'l2>(
        &'a self,
        client: &'l1 Strong<dyn ITouchInjectorClient>,
        slot_id: i32,
        position: &'l2 TouchPosition,
    ) -> binder::Result<()> {
        let client_addr = client.as_binder().as_native() as usize;

        log::debug!(
            "Client {client_addr} requested to start touch at [x: {}, y: {}] on slot {slot_id}",
            position.x,
            position.y
        );

        let Some(slot_range) = self.slot_range_map.read().await.get(client).cloned() else {
            log::warn!("Client {client_addr} attempted to start touch without registering",);
            return Err(Self::invalid_argument_status(
                "Client is not registered for touch injection",
            ));
        };

        if slot_id < 0 || slot_id as usize >= slot_range.len() {
            log::info!(
                "Client {client_addr} tried to start touch on slot {slot_id}, which is outside its assigned range",
            );
            return Err(Self::invalid_argument_status(
                "Slot id is outside the assigned range",
            ));
        }

        let mut device_state_lock = self.device_state.lock().await;
        let Some(device_state) = MutexGuard::deref_mut(&mut device_state_lock).as_mut() else {
            log::error!("Start touch requested before virtual device was initialized");
            return Err(Self::illegal_state_status(
                "Virtual touch device is not initialized",
            ));
        };

        let mut pressed_position_map_lock = self.pressed_position_map.write().await;
        let pressed_position = pressed_position_map_lock
            .get_mut(client)
            .unwrap()
            .get_mut(slot_id as usize)
            .unwrap();

        if pressed_position.is_some() {
            log::warn!("Client {client_addr} attempted to start touch on slot {slot_id} twice",);
            return Err(Self::invalid_argument_status(
                "Touch on this slot is already active",
            ));
        }

        *pressed_position = Some((position.x * 10, position.y * 10));

        if let Err(err) = device_state
            .emulator
            .begin_touch(
                slot_range.start + slot_id as usize,
                position.x * 10,
                position.y * 10,
            )
            .await
        {
            log::warn!("Failed to begin touch!\n{err:?}");
        }

        Ok(())
    }

    async fn r#endTouch<'a, 'l1>(
        &'a self,
        client: &'l1 Strong<dyn ITouchInjectorClient>,
        slot_id: i32,
    ) -> binder::Result<()> {
        let client_addr = client.as_binder().as_native() as usize;
        log::debug!("Client {client_addr} requested to end touch on slot {slot_id}",);

        let Some(slot_range) = self.slot_range_map.read().await.get(client).cloned() else {
            log::warn!("Client {client_addr} attempted to end touch without registering",);
            return Err(Self::invalid_argument_status(
                "Client is not registered for touch injection",
            ));
        };

        if slot_id < 0 || slot_id as usize >= slot_range.len() {
            log::info!(
                "Client {client_addr} tried to end touch on slot {slot_id}, which is outside its assigned range",
            );
            return Err(Self::invalid_argument_status(
                "Slot id is outside the assigned range",
            ));
        }

        let mut device_state_lock = self.device_state.lock().await;
        let Some(device_state) = MutexGuard::deref_mut(&mut device_state_lock).as_mut() else {
            log::error!("End touch requested before virtual device was initialized");
            return Err(Self::illegal_state_status(
                "Virtual touch device is not initialized",
            ));
        };

        let mut pressed_position_map_lock = self.pressed_position_map.write().await;
        let pressed_position = pressed_position_map_lock
            .get_mut(client)
            .unwrap()
            .get_mut(slot_id as usize)
            .unwrap();

        if pressed_position.is_none() {
            log::info!("Client {client_addr} tried to end an inactive touch on slot {slot_id}",);
            return Ok(()); // don't respond with error, bc I want
        }

        *pressed_position = None;

        if let Err(err) = device_state
            .emulator
            .end_touch(slot_range.start + slot_id as usize)
            .await
        {
            log::warn!("Failed to end touch!\n{err:?}");
        }

        Ok(())
    }
}
