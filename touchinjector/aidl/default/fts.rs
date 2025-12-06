use crate::utils::udev::enumerate_devices;
use anyhow::Context;
use evdev_rs::{Device, InputEvent, ReadFlag};
use nix::errno::Errno;
use nix::ioctl_write_int;
use nix::libc::EAGAIN;
use nix::poll::{PollFd, PollFlags, PollTimeout, poll};
use std::fs::OpenOptions;
use std::os::fd::{AsFd, AsRawFd, OwnedFd};
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{Mutex, mpsc};
use tokio::task;

ioctl_write_int!(eviocgrab, b'E', 0x90);

fn working_thread(
    device: Device,
    tx: Sender<InputEvent>,
    cancel_fd: OwnedFd,
    mutex: Arc<Mutex<()>>,
) {
    let _lock = mutex.blocking_lock();

    let mut fds = [
        PollFd::new(device.file().as_fd(), PollFlags::POLLIN),
        PollFd::new(cancel_fd.as_fd(), PollFlags::POLLIN),
    ];

    loop {
        if let Err(e) = poll(&mut fds, PollTimeout::NONE) {
            match e {
                Errno::EINTR => continue,
                e => panic!("poll: {e}"),
            }
        }

        if let Some(re) = fds[1].revents() {
            if re.contains(PollFlags::POLLIN) {
                log::debug!("Received cancel request for FTS worker, shutting down");

                let mut buf = [0u8; 1];
                let _ = nix::unistd::read(cancel_fd.as_raw_fd(), &mut buf);
                break;
            }
        }

        loop {
            let ev = match device.next_event(ReadFlag::BLOCKING) {
                Ok((_, ev)) => ev,
                Err(e) if e.raw_os_error() == Some(EAGAIN) => break,
                Err(e) => panic!("Failed to poll event from fts device: {}", e),
            };

            if let Err(e) = tx.blocking_send(ev) {
                eprintln!("{}", e);
                return;
            }
        }
    }
}

pub struct FtsEventPipe {
    /// Mutex, that will be unlocked after worker stop.
    mutex: Option<Arc<Mutex<()>>>,

    /// Cancel pipe input for stopping worker.
    cancel_w: Option<OwnedFd>,

    /// InputEvent output channel.
    input_event_rx: Option<Receiver<InputEvent>>,
}

impl FtsEventPipe {
    pub fn new() -> anyhow::Result<FtsEventPipe> {
        let mutex = Arc::new(Mutex::new(()));
        let (cancel_r, cancel_w) = nix::unistd::pipe().context("Failed to create pipe")?;

        let (dev_path, _) = enumerate_devices()
            .context("Failed to enumerate devices")?
            .into_iter()
            .find(|(_, name)| name == "fts")
            .ok_or(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Input device with name `fts` not found",
            ))?;

        let file = OpenOptions::new()
            .read(true)
            .open(dev_path)
            .context("Failed to open device")?;
        let fd = file.as_raw_fd();

        unsafe {
            eviocgrab(fd, 1).context("Failed to grab device")?;
        }

        let device = Device::new_from_file(file).context("Failed to create device from file")?;
        let (input_event_tx, input_event_rx) = mpsc::channel::<InputEvent>(4);

        let mutex_clone = Arc::clone(&mutex);
        task::spawn_blocking(move || working_thread(device, input_event_tx, cancel_r, mutex_clone));

        Ok(Self {
            mutex: Some(mutex),
            cancel_w: Some(cancel_w),
            input_event_rx: Some(input_event_rx),
        })
    }

    pub async fn stop(&mut self) {
        let Some(cancel_w) = self.cancel_w.take() else {
            return;
        };

        log::debug!("Requesting FTS event pipe shutdown");

        let _ = nix::unistd::write(&cancel_w, &[1u8]);
        let _ = nix::unistd::close(cancel_w.as_raw_fd());
        let _ = self.mutex.take().unwrap().lock().await;
    }

    pub fn take_input_event_receiver(&mut self) -> Option<Receiver<InputEvent>> {
        self.input_event_rx.take()
    }
}
