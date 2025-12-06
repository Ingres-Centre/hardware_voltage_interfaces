use crate::udev::enumerate_devices;
use anyhow::Context;
use evdev_rs::enums::{EventCode, EV_KEY};
use evdev_rs::{Device, InputEvent, ReadFlag};
use nix::errno::Errno;
use nix::libc::EAGAIN;
use nix::poll::{poll, PollFd, PollFlags, PollTimeout};
use std::os::fd::AsFd;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task;

#[derive(Debug)]
pub enum EventType {
    Open,
    Close,
    Press,
    Release,
}

#[derive(Debug)]
pub struct Event {
    pub r#type: EventType,
    pub slot: u32,
}

fn map_event(ev: InputEvent) -> Option<Event> {
    match ev.event_code {
        EventCode::EV_KEY(key) => match key {
            EV_KEY::KEY_F1 => Some(Event {
                slot: 0,
                r#type: if ev.value == 1 {
                    EventType::Press
                } else {
                    EventType::Release
                },
            }),
            EV_KEY::KEY_F2 => Some(Event {
                slot: 1,
                r#type: if ev.value == 1 {
                    EventType::Press
                } else {
                    EventType::Release
                },
            }),

            EV_KEY::KEY_F3 => {
                if ev.value == 1 {
                    Some(Event {
                        slot: 0,
                        r#type: EventType::Open,
                    })
                } else {
                    None
                }
            }
            EV_KEY::KEY_F4 => {
                if ev.value == 1 {
                    Some(Event {
                        slot: 0,
                        r#type: EventType::Close,
                    })
                } else {
                    None
                }
            }

            EV_KEY::KEY_F5 => {
                if ev.value == 1 {
                    Some(Event {
                        slot: 1,
                        r#type: EventType::Open,
                    })
                } else {
                    None
                }
            }
            EV_KEY::KEY_F6 => {
                if ev.value == 1 {
                    Some(Event {
                        slot: 1,
                        r#type: EventType::Close,
                    })
                } else {
                    None
                }
            }

            _ => None,
        },
        _ => None,
    }
}

fn working_thread(device: Device, tx: Sender<Event>) {
    let fd = device.file().as_fd();
    let mut pfd = [PollFd::new(fd, PollFlags::POLLIN)];

    loop {
        if let Err(e) = poll(&mut pfd, PollTimeout::NONE) {
            match e {
                Errno::EINTR => continue,
                e => panic!("poll: {e}"),
            }
        }

        loop {
            let ev = match device.next_event(ReadFlag::BLOCKING) {
                Ok((_, ev)) => map_event(ev),
                Err(e) if e.raw_os_error() == Some(EAGAIN) => break,
                Err(e) => panic!("Failed to poll event from gamekey device: {}", e),
            };

            #[allow(clippy::collapsible_if)]
            if let Some(ev) = ev {
                if let Err(e) = tx.blocking_send(ev) {
                    eprintln!("{}", e);
                    return;
                }
            }
        }
    }
}

pub fn read_gamekey_events() -> anyhow::Result<Receiver<Event>> {
    let (dev_path, _) = enumerate_devices()
        .context("Failed to enumerate devices")?
        .into_iter()
        .find(|(_, name)| name == "xm_gamekey")
        .ok_or(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "Input device with name `xm_gamekey` not found",
        ))?;

    let device = Device::new_from_path(dev_path).context("Failed to create Device from path")?;
    let (tx, rx) = mpsc::channel::<Event>(4);

    task::spawn_blocking(move || working_thread(device, tx));

    Ok(rx)
}
