use crate::gamekeys::read_gamekey_events;
use anyhow::Context;
use futures::future::err;
use gamekeys::EventType;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::runtime::Runtime;
use tokio::sync::{Mutex, RwLock};
use {
    crate::binder_service::GameKeysService,
    binder_tokio::TokioRuntime,
    log::LevelFilter,
    std::error::Error,
    vendor_lineage_gamekeys::{
        aidl::vendor::lineage::gamekeys::{
            GameKeyEvent::GameKeyEvent, GameKeyEventType::GameKeyEventType, IGameKeys::BnGameKeys,
            IGameKeysEventListener::IGameKeysEventListener,
        },
        binder::BinderFeatures,
    },
};

mod binder_service;
mod gamekeys;
mod udev;

async fn gk_event_loop(
    listeners: Arc<Mutex<Vec<binder::Strong<dyn IGameKeysEventListener>>>>,
) -> anyhow::Result<()> {
    let mut event_stream = read_gamekey_events().context("Get GameKey event stream failed")?;

    loop {
        let Some(ev) = event_stream.recv().await else {
            log::warn!("GameKey event stream is dead!");
            break;
        };

        let aidl_event_type = match &ev.r#type {
            EventType::Open => GameKeyEventType::OPEN,
            EventType::Close => GameKeyEventType::CLOSE,
            EventType::Press => GameKeyEventType::DOWN,
            EventType::Release => GameKeyEventType::UP,
        };

        let aidl_event = GameKeyEvent {
            id: ev.slot as i32,
            r#type: aidl_event_type,
        };

        for listener in listeners.lock().await.iter() {
            listener.onGameKeyEvent(&aidl_event)?;
        }

        log::debug!("{:?}", ev);
    }

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _init_success = logger::init(
        logger::Config::default()
            .with_tag_on_device("vendor.lineage.gamekeys-service")
            .with_max_level(LevelFilter::Trace),
    );

    log::info!("Startup...");

    if let Err(err) = real_main().await {
        log::error!("{:?}", err);
        return Err(err);
    }

    Ok(())
}

async fn real_main() -> anyhow::Result<()> {
    binder::ProcessState::start_thread_pool();
    log::info!("Binder thread pool has been started!");

    let listeners = Arc::new(Mutex::new(
        Vec::<binder::Strong<dyn IGameKeysEventListener>>::new(),
    ));

    let name = "vendor.lineage.gamekeys.IGameKeys/default";
    let svc = BnGameKeys::new_async_binder(
        GameKeysService::new(Arc::clone(&listeners)),
        TokioRuntime(tokio::runtime::Handle::current()),
        BinderFeatures::default(),
    );

    binder::add_service(name, svc.as_binder()).context("Failed to add IGameKeys binder service")?;
    log::info!("Binder service '{}' added successfully!", name);

    gk_event_loop(listeners)
        .await
        .context("Error occurred while reading input events from gamekey device")
}
