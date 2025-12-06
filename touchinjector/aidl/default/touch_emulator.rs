use crate::utils::counter::IncrementalCounter;
use evdev_rs::InputEvent;
use evdev_rs::enums::{EV_ABS, EV_KEY, EV_SYN, EventCode};
use std::fmt;
use std::fmt::Formatter;
use tokio::sync::mpsc::{Receiver, Sender};

pub struct TouchEmulator {
    output: Sender<InputEvent>,
    slot_states: Vec<bool>,
    touch_counter: IncrementalCounter<i32>,
}

#[derive(Debug)]
pub enum Error {
    InvalidSlotCount,
    InvalidSlotId,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Error::InvalidSlotCount => {
                write!(f, "Invalid slot count! Min count is 1, max 20.")
            }
            Error::InvalidSlotId => write!(f, "Invalid slot id!"),
        }
    }
}

impl std::error::Error for Error {}

impl TouchEmulator {
    pub fn new(slot_count: usize) -> anyhow::Result<(Self, Receiver<InputEvent>)> {
        log::debug!("Creating touch emulator with {slot_count} total slots.");
        
        if slot_count == 0 || slot_count > 20 {
            return Err(Error::InvalidSlotCount.into());
        }

        let mut slot_states = Vec::new();
        slot_states.resize(slot_count, false);

        let (tx, rx) = tokio::sync::mpsc::channel::<InputEvent>(4);

        Ok((
            Self {
                output: tx,
                slot_states,
                touch_counter: IncrementalCounter::new(0),
            },
            rx,
        ))
    }

    async fn touch(&mut self, slot: usize, pos: Option<(i32, i32)>) -> anyhow::Result<()> {
        if self.slot_states.len() < slot {
            return Err(Error::InvalidSlotId.into());
        }

        let now = || std::time::SystemTime::now().try_into().unwrap();
        let is_press = pos.is_some();

        if self.slot_states[slot] ^ (!is_press) {
            return Ok(());
        }

        let touched_before = self.slot_states.iter().any(|s| *s);
        self.slot_states[slot] = is_press;
        let touched_after = self.slot_states.iter().any(|s| *s);

        self.output
            .send(InputEvent {
                time: now(),
                event_code: EventCode::EV_ABS(EV_ABS::ABS_MT_SLOT),
                value: slot as i32,
            })
            .await?;

        self.output
            .send(InputEvent {
                time: now(),
                event_code: EventCode::EV_ABS(EV_ABS::ABS_MT_TRACKING_ID),
                value: if is_press {
                    self.touch_counter.next()
                } else {
                    -1
                },
            })
            .await?;

        if (is_press && !touched_before) || (!is_press && !touched_after) {
            self.output
                .send(InputEvent {
                    time: now(),
                    event_code: EventCode::EV_KEY(EV_KEY::BTN_TOUCH),
                    value: is_press as i32,
                })
                .await?;

            self.output
                .send(InputEvent {
                    time: now(),
                    event_code: EventCode::EV_KEY(EV_KEY::BTN_TOOL_FINGER),
                    value: is_press as i32,
                })
                .await?;
        }

        if let Some((x, y)) = pos {
            self.output
                .send(InputEvent {
                    time: now(),
                    event_code: EventCode::EV_ABS(EV_ABS::ABS_MT_POSITION_X),
                    value: x,
                })
                .await?;

            self.output
                .send(InputEvent {
                    time: now(),
                    event_code: EventCode::EV_ABS(EV_ABS::ABS_MT_POSITION_Y),
                    value: y,
                })
                .await?;
        }

        self.output
            .send(InputEvent {
                time: now(),
                event_code: EventCode::EV_SYN(EV_SYN::SYN_REPORT),
                value: 0,
            })
            .await?;

        Ok(())
    }

    pub async fn begin_touch(&mut self, slot: usize, x: i32, y: i32) -> anyhow::Result<()> {
        log::debug!("Starting touch at [x: {x}, y: {y}] on slot {slot}");
        self.touch(slot, Some((x, y))).await
    }

    pub async fn end_touch(&mut self, slot: usize) -> anyhow::Result<()> {
        log::debug!("Ending touch on slot {slot}");
        self.touch(slot, None).await
    }

    pub async fn end_all_touches(&mut self) {
        for slot_id in 0..self.slot_states.len() {
            if !self.slot_states[slot_id] {
                continue;
            }

            log::debug!("Clearing active touch on slot {slot_id}");
            let _ = self.touch(slot_id, None).await;
        }
    }
}
