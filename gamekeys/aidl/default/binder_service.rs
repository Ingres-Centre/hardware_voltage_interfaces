use async_trait::async_trait;
use binder::unstable_api::AsNative;
use binder::{ExceptionCode, Interface, Result, Status, Strong};
use std::sync::Arc;
use tokio::sync::Mutex;
use vendor_lineage_gamekeys::aidl::vendor::lineage::gamekeys::{
    IGameKeys::IGameKeysAsyncServer, IGameKeysEventListener::IGameKeysEventListener,
};

pub struct GameKeysService {
    listeners: Arc<Mutex<Vec<Strong<dyn IGameKeysEventListener>>>>,
}

impl Interface for GameKeysService {}

impl GameKeysService {
    pub fn new(listeners: Arc<Mutex<Vec<Strong<dyn IGameKeysEventListener>>>>) -> Self {
        Self { listeners }
    }

    fn invalid_argument_status(message: &str) -> Status {
        Status::new_exception_str(ExceptionCode::ILLEGAL_ARGUMENT, Some(message))
    }
}

#[allow(non_snake_case)]
#[async_trait]
impl IGameKeysAsyncServer for GameKeysService {
    async fn r#registerEventListener<'a, 'l1>(
        &'a self,
        listener: &'l1 Strong<dyn IGameKeysEventListener>,
    ) -> Result<()> {
        let listener_addr = listener.as_binder().as_native() as usize;
        log::debug!("Registering GameKey listener {listener_addr}");

        let mut listeners_lock = self.listeners.lock().await;

        if listeners_lock.contains(listener) {
            log::warn!("GameKey listener {listener_addr} is already registered");
            return Err(Self::invalid_argument_status("Listener is already registered"));
        }

        listeners_lock.push(Strong::clone(listener));

        Ok(())
    }

    async fn r#unregisterEventListener<'a, 'l1>(
        &'a self,
        listener: &'l1 Strong<dyn IGameKeysEventListener>,
    ) -> Result<()> {
        let listener_addr = listener.as_binder().as_native() as usize;
        log::debug!("Unregistering GameKey listener {listener_addr}");

        let mut listeners_lock = self.listeners.lock().await;

        if let Some(index) = listeners_lock
            .iter()
            .enumerate()
            .find(|(index, item)| Strong::eq(item, listener))
            .map(|(index, _)| index)
        {
            listeners_lock.remove(index);
        } else {
            log::warn!("GameKey listener {listener_addr} is not registered");
            return Err(Self::invalid_argument_status(
                "Listener is not registered",
            ));
        }

        Ok(())
    }
}
