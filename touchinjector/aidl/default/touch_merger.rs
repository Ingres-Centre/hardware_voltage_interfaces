use crate::utils::counter::IncrementalCounter;
use anyhow::Context;
use evdev_rs::enums::{BusType, EventCode, EventType, InputProp, EV_ABS, EV_KEY, EV_SYN};
use evdev_rs::{AbsInfo, DeviceWrapper, EnableCodeData, InputEvent, UInputDevice, UninitDevice};
use futures::StreamExt;
use std::cell::RefCell;
use tokio::sync::mpsc::Receiver;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamMap;
use tokio_util::sync::CancellationToken;

pub struct TouchSourceDeclaration {
    pub slot_count: usize,
}

pub struct TouchSourceState {
    pub event_buffer: Vec<InputEvent>,
    pub current_slot: usize,
    pub in_touch: bool,
}

pub struct TouchMerger {
    idev_decls: Box<[TouchSourceDeclaration]>,
    idev_states: Box<[RefCell<TouchSourceState>]>,

    output_device: UInputDevice,
    stream_map: StreamMap<usize, ReceiverStream<InputEvent>>,
    current_slot: i32,
    tracking_id: IncrementalCounter<i32>,
}

impl TouchSourceDeclaration {
    pub fn new(slot_count: usize) -> Self {
        Self { slot_count }
    }
}

impl TouchSourceState {
    pub fn new() -> Self {
        Self {
            in_touch: false,
            current_slot: 0,
            event_buffer: Vec::new(),
        }
    }

    pub fn try_get_complete_event(&mut self, event: InputEvent) -> Option<Box<[InputEvent]>> {
        let code = event.event_code;
        self.event_buffer.push(event);

        if code == EventCode::EV_SYN(EV_SYN::SYN_REPORT) {
            // why Vec doesn't have take method?
            return Some(std::mem::take(&mut self.event_buffer).into_boxed_slice());
        }

        None
    }
}

impl TouchMerger {
    fn create_input_device(slot_count: i32) -> anyhow::Result<UInputDevice> {
        if slot_count <= 0 || slot_count > 20 {
            return Err(anyhow::Error::msg("slot count > 20 or <= 0"));
        }

        let u = UninitDevice::new().unwrap();

        u.set_name("touch-injector");
        u.set_bustype(BusType::BUS_VIRTUAL as u16);
        u.set_vendor_id(0xFFFF);
        u.set_product_id(0x1111);

        u.enable(EventType::EV_KEY)?;
        u.enable(EventType::EV_ABS)?;
        u.enable_property(&InputProp::INPUT_PROP_DIRECT)?;

        u.enable(EventCode::EV_SYN(EV_SYN::SYN_REPORT))?;
        u.enable(EventCode::EV_SYN(EV_SYN::SYN_MT_REPORT))?;

        u.enable(EventCode::EV_KEY(EV_KEY::BTN_TOUCH))?;
        u.enable(EventCode::EV_KEY(EV_KEY::BTN_TOOL_FINGER))?;

        let abs = |min: i32, max: i32| {
            EnableCodeData::AbsInfo(AbsInfo {
                value: 0,
                minimum: min,
                maximum: max,
                flat: 0,
                fuzz: 0,
                resolution: 0,
            })
        };

        u.enable_event_code(
            &EventCode::EV_ABS(EV_ABS::ABS_MT_SLOT),
            Some(abs(0, slot_count - 1)),
        )?;
        u.enable_event_code(
            &EventCode::EV_ABS(EV_ABS::ABS_MT_TOUCH_MAJOR),
            Some(abs(0, 10800)),
        )?;
        u.enable_event_code(
            &EventCode::EV_ABS(EV_ABS::ABS_MT_TOUCH_MINOR),
            Some(abs(0, 24000)),
        )?;
        u.enable_event_code(
            &EventCode::EV_ABS(EV_ABS::ABS_MT_WIDTH_MAJOR),
            Some(abs(0, 127)),
        )?;
        u.enable_event_code(
            &EventCode::EV_ABS(EV_ABS::ABS_MT_WIDTH_MINOR),
            Some(abs(0, 127)),
        )?;
        u.enable_event_code(
            &EventCode::EV_ABS(EV_ABS::ABS_MT_ORIENTATION),
            Some(abs(-90, 90)),
        )?;
        u.enable_event_code(
            &EventCode::EV_ABS(EV_ABS::ABS_MT_POSITION_X),
            Some(abs(0, 10799)),
        )?;
        u.enable_event_code(
            &EventCode::EV_ABS(EV_ABS::ABS_MT_POSITION_Y),
            Some(abs(0, 23999)),
        )?;
        u.enable_event_code(
            &EventCode::EV_ABS(EV_ABS::ABS_MT_TRACKING_ID),
            Some(abs(0, 65535)),
        )?;
        u.enable_event_code(
            &EventCode::EV_ABS(EV_ABS::ABS_MT_DISTANCE),
            Some(abs(0, 127)),
        )?;

        UInputDevice::create_from_device(&u).context("Failed to create UInputDevice from Device")
    }
    pub fn new(
        sources: Box<[(TouchSourceDeclaration, Receiver<InputEvent>)]>,
    ) -> anyhow::Result<Self> {
        let mut stream_map = StreamMap::<usize, _>::new();

        let decls: Box<[TouchSourceDeclaration]> = Vec::from(sources)
            .into_iter()
            .enumerate()
            .map(|(i, (d, s))| {
                stream_map.insert(i, ReceiverStream::new(s));
                d
            })
            .collect();

        let output_device =
            Self::create_input_device(decls.iter().map(|d| d.slot_count).sum::<usize>() as i32)
                .context("Failed to create input device for TouchMerger")?;

        Ok(Self {
            idev_states: decls
                .iter()
                .map(|_| RefCell::new(TouchSourceState::new()))
                .collect(),
            idev_decls: decls,
            output_device,
            stream_map,
            current_slot: 0,
            tracking_id: IncrementalCounter::new(0),
        })
    }

    fn any_touched_except(&self, index: usize) -> bool {
        self.idev_states
            .iter()
            .enumerate()
            .any(|(i, d)| i != index && d.borrow().in_touch)
    }

    fn get_slot_with_offset(&self, index: usize, slot: i32) -> i32 {
        let mut slot = slot;

        for i in 0..index {
            slot += self.idev_decls[i].slot_count as i32;
        }

        slot
    }

    pub async fn processing_task(mut self, cancel: CancellationToken) -> anyhow::Result<()> {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    log::debug!("Received cancellation token event! Stopping...");
                    break;
                }
                ev = self.stream_map.next() => {
                    let Some((key, val)) = ev else {
                        break;
                    };

                    let mut state = self.idev_states[key].borrow_mut();

                    let Some(events) = state.try_get_complete_event(val) else {
                        continue;
                    };

                    let mut new_events = Vec::<InputEvent>::new();

                    for mut event in events {
                        match &event.event_code {
                            EventCode::EV_ABS(EV_ABS::ABS_MT_SLOT) => {
                                let slot = self.get_slot_with_offset(key, event.value);

                                state.current_slot = slot as usize;
                                self.current_slot = slot;
                                event.value = slot;
                            }
                            EventCode::EV_ABS(EV_ABS::ABS_MT_TRACKING_ID) => {
                                if event.value != 0xFFFFFFFFu32 as i32 {
                                    event.value = self.tracking_id.next();
                                }
                            }

                            EventCode::EV_KEY(EV_KEY::BTN_TOUCH)
                            | EventCode::EV_KEY(EV_KEY::BTN_TOOL_FINGER) => {
                                state.in_touch = event.value == 1;

                                if self.any_touched_except(key) {
                                    continue;
                                }
                            }

                            _ => {}
                        }

                        if self.current_slot != state.current_slot as i32 {
                            new_events.push(InputEvent {
                                time: std::time::SystemTime::now().try_into()?,
                                event_code: EventCode::EV_ABS(EV_ABS::ABS_MT_SLOT),
                                value: state.current_slot as i32,
                            });

                            self.current_slot = state.current_slot as i32;
                        }

                        event.time = std::time::SystemTime::now().try_into()?;
                        new_events.push(event);
                    }

                    for event in &new_events {
                        self.output_device
                            .write_event(event)
                            .context("Failed to write to output device")?;
                    }
                }
            }
        }

        Ok(())
    }
}
