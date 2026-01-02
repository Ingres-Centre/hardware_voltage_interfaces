use anyhow::Context;
use futures::future::err;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::runtime::Runtime;
use tokio::sync::{Mutex, RwLock};
use {
    crate::binder_service::LedsService,
    binder_tokio::TokioRuntime,
    log::LevelFilter,
    std::error::Error,
    vendor_lineage_leds::{
        aidl::vendor::lineage::leds::ILeds::BnLeds,
        binder::BinderFeatures,
    },
};

mod binder_service;
mod awinic_sysfs;

fn main() -> anyhow::Result<()> {
    let _init_success = logger::init(
        logger::Config::default()
            .with_tag_on_device("vendor.lineage.leds-service")
            .with_max_level(LevelFilter::Debug),
    );

    log::info!("Startup...");

    let runtime = Runtime::new()?;

    binder::ProcessState::start_thread_pool();
    log::info!("Binder thread pool has been started!");

    let name = "vendor.lineage.leds.ILeds/default";
    let async_binder = BnLeds::new_async_binder(
        LedsService,
        TokioRuntime(runtime.handle().clone()),
        BinderFeatures::default(),
    );

    binder::add_service(name, async_binder.as_binder())
        .context("Failed to add ILeds binder service")?;
    log::info!("Binder service '{}' added successfully!", name);

    binder::ProcessState::join_thread_pool();

    Ok(())
}
