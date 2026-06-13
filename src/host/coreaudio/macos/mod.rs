#![allow(deprecated)]
use std::{
    sync::{mpsc, Arc, Mutex, Weak},
    time::Duration,
};

use coreaudio::audio_unit::AudioUnit;
use objc2_core_audio::{
    kAudioAggregateDevicePropertyActiveSubDeviceList, kAudioAggregateDevicePropertyClockDevice,
    kAudioAggregateDevicePropertyComposition, kAudioAggregateDevicePropertyFullSubDeviceList,
    kAudioAggregateDevicePropertyMainSubDevice, kAudioAggregateDevicePropertySubTapList,
    kAudioAggregateDevicePropertyTapList, kAudioDevicePropertyDeviceIsAlive,
    kAudioDevicePropertyNominalSampleRate, kAudioDevicePropertyStreamConfiguration,
    kAudioDevicePropertyStreamFormat, kAudioHardwarePropertyDefaultOutputDevice,
    kAudioHardwarePropertyDevices, kAudioHardwarePropertyTapList, kAudioObjectPropertyElementMain,
    kAudioObjectPropertyScopeGlobal, kAudioObjectPropertyScopeInput,
    kAudioObjectPropertyScopeOutput, kAudioObjectSystemObject, kAudioTapPropertyDescription,
    kAudioTapPropertyFormat, AudioDeviceID, AudioObjectID, AudioObjectPropertyAddress,
    AudioObjectPropertyScope, AudioObjectPropertySelector,
};
use property_listener::AudioObjectPropertyListener;

pub use self::enumerate::{default_input_device, default_output_device, Devices};
use super::{asbd_from_config, check_os_status, host_time_to_stream_instant, OSStatus};
use crate::{
    host::{coreaudio::macos::loopback::LoopbackDevice, emit_error, latch::Latch},
    traits::{HostTrait, StreamTrait},
    Error, ErrorKind, FrameCount, ResultExt, StreamInstant,
};

mod device;
pub mod enumerate;
mod loopback;
mod property_listener;
pub use device::Device;

/// Coreaudio host, the default host on macOS.
#[derive(Debug)]
pub struct Host;

impl Host {
    pub fn new() -> Result<Self, Error> {
        Ok(Host)
    }
}

impl HostTrait for Host {
    type Devices = Devices;
    type Device = Device;

    fn is_available() -> bool {
        // Assume coreaudio is always available
        true
    }

    fn devices(&self) -> Result<Self::Devices, Error> {
        Devices::new()
    }

    fn default_input_device(&self) -> Option<Self::Device> {
        default_input_device()
    }

    fn default_output_device(&self) -> Option<Self::Device> {
        default_output_device()
    }

    fn system_audio_device(&self) -> Option<Self::Device> {
        // The global tap is not bound to a device; carry the current default output id as a
        // harmless placeholder (the tap itself never uses it).
        let placeholder = default_output_device().map(|d| d.audio_device_id).unwrap_or(0);
        Some(Device::system_audio(placeholder))
    }
}

/// Type alias for the error callback to reduce complexity
type ErrorCallback = dyn FnMut(Error) + Send;

/// Spawns a dedicated thread that registers a single property listener and signals a channel on
/// each change. The listener is deregistered when the returned `Sender<()>` is dropped.
fn spawn_property_listener_thread(
    object_id: AudioObjectID,
    address: AudioObjectPropertyAddress,
) -> Result<(mpsc::Receiver<()>, mpsc::Sender<()>), Error> {
    let (shutdown_tx, shutdown_rx) = mpsc::channel::<()>();
    let (change_tx, change_rx) = mpsc::channel::<()>();
    let (ready_tx, ready_rx) = mpsc::channel();

    std::thread::spawn(move || {
        let listener = AudioObjectPropertyListener::new(object_id, address, move || {
            let _ = change_tx.send(());
        });
        match listener {
            Ok(_l) => {
                let _ = ready_tx.send(Ok(()));
                let _ = shutdown_rx.recv();
            }
            Err(e) => {
                let _ = ready_tx.send(Err(e));
            }
        }
    });

    ready_rx.recv().map_err(|_| {
        Error::with_message(
            ErrorKind::StreamInvalidated,
            "property listener thread terminated unexpectedly",
        )
    })??;

    Ok((change_rx, shutdown_tx))
}

/// A device monitor that can signal when the owning `Stream` handle has been returned to the
/// caller, allowing the delivery thread to start processing events.
pub(super) trait Monitor: Send + Sync {
    /// Unblocks the delivery thread. Called after `Stream::new()` and from `Stream::drop()`.
    fn signal_ready(&self);
}

/// Manages device disconnection listener on a dedicated thread to ensure the
/// AudioObjectPropertyListener is always created and dropped on the same thread.
/// This avoids potential threading issues with CoreAudio APIs.
///
/// When a device disconnects, this manager:
/// 1. Attempts to pause the stream to stop audio I/O
/// 2. Calls the error callback with `ErrorKind::DeviceNotAvailable`
///
/// The dedicated thread architecture ensures `Stream` can implement `Send`.
struct DisconnectManager {
    latch: Latch,
    _shutdown_tx: mpsc::Sender<()>,
}

impl DisconnectManager {
    fn new(
        device_id: AudioDeviceID,
        stream_weak: Weak<Mutex<StreamInner>>,
        error_callback: Arc<Mutex<ErrorCallback>>,
    ) -> Result<Self, Error> {
        let (shutdown_tx, shutdown_rx) = mpsc::channel();
        let (disconnect_tx, disconnect_rx) = mpsc::channel::<Error>();
        let (ready_tx, ready_rx) = mpsc::channel();

        // Spawn a dedicated thread to own both listeners. CoreAudio requires that
        // AudioObjectPropertyListeners are added and removed on the same thread.
        let disconnect_tx_alive = disconnect_tx.clone();
        let disconnect_tx_rate = disconnect_tx;
        std::thread::spawn(move || {
            let alive_address = AudioObjectPropertyAddress {
                mSelector: kAudioDevicePropertyDeviceIsAlive,
                mScope: kAudioObjectPropertyScopeGlobal,
                mElement: kAudioObjectPropertyElementMain,
            };
            let alive_listener =
                AudioObjectPropertyListener::new(device_id, alive_address, move || {
                    let _ = disconnect_tx_alive.send(Error::with_message(
                        ErrorKind::DeviceNotAvailable,
                        "Device disconnected",
                    ));
                });

            let rate_address = AudioObjectPropertyAddress {
                mSelector: kAudioDevicePropertyNominalSampleRate,
                mScope: kAudioObjectPropertyScopeGlobal,
                mElement: kAudioObjectPropertyElementMain,
            };
            let rate_listener =
                AudioObjectPropertyListener::new(device_id, rate_address, move || {
                    let _ = disconnect_tx_rate.send(Error::with_message(
                        ErrorKind::StreamInvalidated,
                        "Device sample rate changed",
                    ));
                });

            match (alive_listener, rate_listener) {
                (Ok(_alive), Ok(_rate)) => {
                    let _ = ready_tx.send(Ok(()));
                    // Block until the stream is dropped; listeners are removed on drop.
                    let _ = shutdown_rx.recv();
                }
                (Err(e), _) | (_, Err(e)) => {
                    let _ = ready_tx.send(Err(e));
                }
            }
        });

        ready_rx.recv().map_err(|_| {
            Error::with_message(
                ErrorKind::StreamInvalidated,
                "Stream monitor terminated unexpectedly",
            )
        })??;

        let mut latch = Latch::new();
        let waiter = latch.waiter();

        let handle = std::thread::Builder::new()
            .name("cpal-coreaudio-disconnect".into())
            .spawn(move || {
                // If the Latch is dropped without being released (error path), exit cleanly.
                if !waiter.wait() {
                    return;
                }
                while let Ok(err) = disconnect_rx.recv() {
                    if let Some(stream_arc) = stream_weak.upgrade() {
                        if let Ok(mut stream_inner) = stream_arc.try_lock() {
                            let _ = stream_inner.pause();
                        }
                        emit_error(&error_callback, err);
                    } else {
                        break;
                    }
                }
            })
            .map_err(|e| {
                Error::with_message(
                    ErrorKind::ResourceExhausted,
                    format!("Failed to spawn disconnect thread: {e}"),
                )
            })?;

        latch.add_thread(handle.thread().clone());
        Ok(DisconnectManager {
            latch,
            _shutdown_tx: shutdown_tx,
        })
    }
}

impl Monitor for DisconnectManager {
    fn signal_ready(&self) {
        self.latch.release();
    }
}

/// Manages the system default output device change listener on a dedicated thread.
///
/// When the system default output device changes:
/// - If a new valid default exists, AudioUnit reroutes and `DeviceChanged` is reported.
/// - If there is no new default, the stream is paused and `DeviceNotAvailable` is reported.
struct DefaultOutputMonitor {
    latch: Latch,
    _shutdown_tx: mpsc::Sender<()>,
}

impl DefaultOutputMonitor {
    fn new(
        stream_weak: Weak<Mutex<StreamInner>>,
        error_callback: Arc<Mutex<ErrorCallback>>,
    ) -> Result<Self, Error> {
        let (change_rx, shutdown_tx) = spawn_property_listener_thread(
            kAudioObjectSystemObject as AudioObjectID,
            AudioObjectPropertyAddress {
                mSelector: kAudioHardwarePropertyDefaultOutputDevice,
                mScope: kAudioObjectPropertyScopeGlobal,
                mElement: kAudioObjectPropertyElementMain,
            },
        )?;

        let mut latch = Latch::new();
        let waiter = latch.waiter();

        let handle = std::thread::Builder::new()
            .name("cpal-coreaudio-default-output".into())
            .spawn(move || {
                if !waiter.wait() {
                    return;
                }
                while let Ok(()) = change_rx.recv() {
                    let Some(arc) = stream_weak.upgrade() else {
                        break;
                    };
                    if default_output_device().is_none() {
                        if let Ok(mut inner) = arc.try_lock() {
                            let _ = inner.pause();
                        }
                        emit_error(
                            &error_callback,
                            Error::with_message(
                                ErrorKind::DeviceNotAvailable,
                                "no default output device",
                            ),
                        );
                    } else {
                        // DefaultOutput AudioUnit rerouted automatically; notify the caller.
                        emit_error(
                            &error_callback,
                            Error::with_message(
                                ErrorKind::DeviceChanged,
                                "default output device changed",
                            ),
                        );
                    }
                }
            })
            .map_err(|e| {
                Error::with_message(
                    ErrorKind::ResourceExhausted,
                    format!("failed to spawn default-output monitor thread: {e}"),
                )
            })?;

        latch.add_thread(handle.thread().clone());
        Ok(DefaultOutputMonitor {
            latch,
            _shutdown_tx: shutdown_tx,
        })
    }
}

impl Monitor for DefaultOutputMonitor {
    fn signal_ready(&self) {
        self.latch.release();
    }
}

#[derive(Copy, Clone, Debug)]
enum SystemAudioListenerEvent {
    DeviceNotAvailable(&'static str),
    StreamInvalidated(&'static str),
    DefaultOutputChanged,
}

#[derive(Debug)]
enum SystemAudioMonitorEvent {
    DeviceNotAvailable(&'static str),
    StreamInvalidated(&'static str),
}

fn check_shutdown(shutdown_rx: &mpsc::Receiver<()>) -> bool {
    match shutdown_rx.try_recv() {
        Ok(()) | Err(mpsc::TryRecvError::Disconnected) => true,
        Err(mpsc::TryRecvError::Empty) => false,
    }
}

fn property_address(
    selector: AudioObjectPropertySelector,
    scope: AudioObjectPropertyScope,
) -> AudioObjectPropertyAddress {
    AudioObjectPropertyAddress {
        mSelector: selector,
        mScope: scope,
        mElement: kAudioObjectPropertyElementMain,
    }
}

fn property_listener(
    object_id: AudioObjectID,
    selector: AudioObjectPropertySelector,
    scope: AudioObjectPropertyScope,
    tx: mpsc::Sender<SystemAudioListenerEvent>,
    event: SystemAudioListenerEvent,
) -> Result<AudioObjectPropertyListener, Error> {
    AudioObjectPropertyListener::new(object_id, property_address(selector, scope), move || {
        let _ = tx.send(event);
    })
}

fn push_optional_listener(
    listeners: &mut Vec<AudioObjectPropertyListener>,
    listener: Result<AudioObjectPropertyListener, Error>,
) {
    if let Ok(listener) = listener {
        listeners.push(listener);
    }
}

fn aggregate_property_listeners(
    aggregate_device_id: AudioDeviceID,
    tx: mpsc::Sender<SystemAudioListenerEvent>,
) -> Result<Vec<AudioObjectPropertyListener>, Error> {
    let mut listeners = Vec::new();
    listeners.push(property_listener(
        aggregate_device_id,
        kAudioDevicePropertyDeviceIsAlive,
        kAudioObjectPropertyScopeGlobal,
        tx.clone(),
        SystemAudioListenerEvent::DeviceNotAvailable("system audio aggregate device disconnected"),
    )?);
    listeners.push(property_listener(
        aggregate_device_id,
        kAudioDevicePropertyNominalSampleRate,
        kAudioObjectPropertyScopeGlobal,
        tx.clone(),
        SystemAudioListenerEvent::StreamInvalidated("system audio aggregate sample rate changed"),
    )?);

    push_optional_listener(
        &mut listeners,
        property_listener(
            aggregate_device_id,
            kAudioDevicePropertyStreamFormat,
            kAudioObjectPropertyScopeInput,
            tx.clone(),
            SystemAudioListenerEvent::StreamInvalidated(
                "system audio aggregate input stream format changed",
            ),
        ),
    );
    push_optional_listener(
        &mut listeners,
        property_listener(
            aggregate_device_id,
            kAudioDevicePropertyStreamConfiguration,
            kAudioObjectPropertyScopeInput,
            tx.clone(),
            SystemAudioListenerEvent::StreamInvalidated(
                "system audio aggregate input stream configuration changed",
            ),
        ),
    );
    push_optional_listener(
        &mut listeners,
        property_listener(
            aggregate_device_id,
            kAudioAggregateDevicePropertyFullSubDeviceList,
            kAudioObjectPropertyScopeGlobal,
            tx.clone(),
            SystemAudioListenerEvent::StreamInvalidated(
                "system audio aggregate subdevice list changed",
            ),
        ),
    );
    push_optional_listener(
        &mut listeners,
        property_listener(
            aggregate_device_id,
            kAudioAggregateDevicePropertyActiveSubDeviceList,
            kAudioObjectPropertyScopeGlobal,
            tx.clone(),
            SystemAudioListenerEvent::StreamInvalidated(
                "system audio aggregate active subdevice list changed",
            ),
        ),
    );
    push_optional_listener(
        &mut listeners,
        property_listener(
            aggregate_device_id,
            kAudioAggregateDevicePropertyTapList,
            kAudioObjectPropertyScopeGlobal,
            tx.clone(),
            SystemAudioListenerEvent::StreamInvalidated("system audio aggregate tap list changed"),
        ),
    );
    push_optional_listener(
        &mut listeners,
        property_listener(
            aggregate_device_id,
            kAudioAggregateDevicePropertySubTapList,
            kAudioObjectPropertyScopeGlobal,
            tx.clone(),
            SystemAudioListenerEvent::StreamInvalidated(
                "system audio aggregate subtap list changed",
            ),
        ),
    );
    push_optional_listener(
        &mut listeners,
        property_listener(
            aggregate_device_id,
            kAudioAggregateDevicePropertyComposition,
            kAudioObjectPropertyScopeGlobal,
            tx.clone(),
            SystemAudioListenerEvent::StreamInvalidated(
                "system audio aggregate composition changed",
            ),
        ),
    );
    push_optional_listener(
        &mut listeners,
        property_listener(
            aggregate_device_id,
            kAudioAggregateDevicePropertyMainSubDevice,
            kAudioObjectPropertyScopeGlobal,
            tx.clone(),
            SystemAudioListenerEvent::StreamInvalidated(
                "system audio aggregate main subdevice changed",
            ),
        ),
    );
    push_optional_listener(
        &mut listeners,
        property_listener(
            aggregate_device_id,
            kAudioAggregateDevicePropertyClockDevice,
            kAudioObjectPropertyScopeGlobal,
            tx,
            SystemAudioListenerEvent::StreamInvalidated(
                "system audio aggregate clock device changed",
            ),
        ),
    );

    Ok(listeners)
}

fn tap_property_listeners(
    tap_id: AudioObjectID,
    tx: mpsc::Sender<SystemAudioListenerEvent>,
) -> Vec<AudioObjectPropertyListener> {
    let mut listeners = Vec::new();
    push_optional_listener(
        &mut listeners,
        property_listener(
            tap_id,
            kAudioTapPropertyFormat,
            kAudioObjectPropertyScopeGlobal,
            tx.clone(),
            SystemAudioListenerEvent::StreamInvalidated("system audio tap format changed"),
        ),
    );
    push_optional_listener(
        &mut listeners,
        property_listener(
            tap_id,
            kAudioTapPropertyDescription,
            kAudioObjectPropertyScopeGlobal,
            tx,
            SystemAudioListenerEvent::StreamInvalidated("system audio tap description changed"),
        ),
    );
    listeners
}

fn system_audio_global_property_listeners(
    tx: mpsc::Sender<SystemAudioListenerEvent>,
) -> Result<Vec<AudioObjectPropertyListener>, Error> {
    let mut listeners = Vec::new();
    listeners.push(property_listener(
        kAudioObjectSystemObject as AudioObjectID,
        kAudioHardwarePropertyDefaultOutputDevice,
        kAudioObjectPropertyScopeGlobal,
        tx.clone(),
        SystemAudioListenerEvent::DefaultOutputChanged,
    )?);
    push_optional_listener(
        &mut listeners,
        property_listener(
            kAudioObjectSystemObject as AudioObjectID,
            kAudioHardwarePropertyDevices,
            kAudioObjectPropertyScopeGlobal,
            tx.clone(),
            SystemAudioListenerEvent::StreamInvalidated(
                "system audio hardware device list changed",
            ),
        ),
    );
    push_optional_listener(
        &mut listeners,
        property_listener(
            kAudioObjectSystemObject as AudioObjectID,
            kAudioHardwarePropertyTapList,
            kAudioObjectPropertyScopeGlobal,
            tx,
            SystemAudioListenerEvent::StreamInvalidated("system audio hardware tap list changed"),
        ),
    );

    Ok(listeners)
}

fn default_output_property_listeners(
    default_output_id: Option<AudioDeviceID>,
    tx: mpsc::Sender<SystemAudioListenerEvent>,
) -> Vec<AudioObjectPropertyListener> {
    let Some(default_output_id) = default_output_id else {
        return Vec::new();
    };

    let mut listeners = Vec::new();
    push_optional_listener(
        &mut listeners,
        property_listener(
            default_output_id,
            kAudioDevicePropertyNominalSampleRate,
            kAudioObjectPropertyScopeGlobal,
            tx.clone(),
            SystemAudioListenerEvent::StreamInvalidated(
                "system audio default output sample rate changed",
            ),
        ),
    );
    push_optional_listener(
        &mut listeners,
        property_listener(
            default_output_id,
            kAudioDevicePropertyStreamFormat,
            kAudioObjectPropertyScopeOutput,
            tx.clone(),
            SystemAudioListenerEvent::StreamInvalidated(
                "system audio default output stream format changed",
            ),
        ),
    );
    push_optional_listener(
        &mut listeners,
        property_listener(
            default_output_id,
            kAudioDevicePropertyStreamConfiguration,
            kAudioObjectPropertyScopeOutput,
            tx.clone(),
            SystemAudioListenerEvent::StreamInvalidated(
                "system audio default output stream configuration changed",
            ),
        ),
    );
    push_optional_listener(
        &mut listeners,
        property_listener(
            default_output_id,
            kAudioDevicePropertyDeviceIsAlive,
            kAudioObjectPropertyScopeGlobal,
            tx,
            SystemAudioListenerEvent::StreamInvalidated(
                "system audio default output availability changed",
            ),
        ),
    );

    listeners
}

fn spawn_system_audio_monitor_thread(
    aggregate_device_id: AudioDeviceID,
    tap_id: AudioObjectID,
) -> Result<(mpsc::Receiver<SystemAudioMonitorEvent>, mpsc::Sender<()>), Error> {
    let (shutdown_tx, shutdown_rx) = mpsc::channel();
    let (monitor_tx, monitor_rx) = mpsc::channel();
    let (listener_tx, listener_rx) = mpsc::channel();
    let (ready_tx, ready_rx) = mpsc::channel();

    std::thread::spawn(move || {
        let mut _listeners =
            match aggregate_property_listeners(aggregate_device_id, listener_tx.clone()) {
                Ok(listeners) => listeners,
                Err(err) => {
                    let _ = ready_tx.send(Err(err));
                    return;
                }
            };
        match system_audio_global_property_listeners(listener_tx.clone()) {
            Ok(mut listeners) => _listeners.append(&mut listeners),
            Err(err) => {
                let _ = ready_tx.send(Err(err));
                return;
            }
        }
        _listeners.append(&mut tap_property_listeners(tap_id, listener_tx.clone()));

        let mut default_output_id = default_output_device().map(|d| d.audio_device_id);
        let mut _default_output_listeners =
            default_output_property_listeners(default_output_id, listener_tx.clone());

        let _ = ready_tx.send(Ok(()));

        loop {
            if check_shutdown(&shutdown_rx) {
                break;
            }

            let event = match listener_rx.recv_timeout(Duration::from_millis(100)) {
                Ok(event) => event,
                Err(mpsc::RecvTimeoutError::Timeout) => continue,
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            };

            match event {
                SystemAudioListenerEvent::DeviceNotAvailable(message) => {
                    let _ = monitor_tx.send(SystemAudioMonitorEvent::DeviceNotAvailable(message));
                }
                SystemAudioListenerEvent::StreamInvalidated(message) => {
                    let _ = monitor_tx.send(SystemAudioMonitorEvent::StreamInvalidated(message));
                }
                SystemAudioListenerEvent::DefaultOutputChanged => {
                    default_output_id = default_output_device().map(|d| d.audio_device_id);
                    _default_output_listeners =
                        default_output_property_listeners(default_output_id, listener_tx.clone());
                    if default_output_id.is_some() {
                        let _ = monitor_tx.send(SystemAudioMonitorEvent::StreamInvalidated(
                            "system audio default output changed",
                        ));
                    } else {
                        let _ = monitor_tx.send(SystemAudioMonitorEvent::DeviceNotAvailable(
                            "no default output device for system audio",
                        ));
                    }
                }
            }
        }
    });

    ready_rx.recv().map_err(|_| {
        Error::with_message(
            ErrorKind::StreamInvalidated,
            "system-audio monitor thread terminated unexpectedly",
        )
    })??;

    Ok((monitor_rx, shutdown_tx))
}

/// Watches the moving clock/format inputs behind the global system-audio tap.
///
/// The placeholder `Device` id is not meaningful for the tap. The live IO surfaces are the
/// private aggregate device, the tap object, the current default output's format, and CoreAudio's
/// hardware/tap lists. Bluetooth profile flips can change any of those without looking like a
/// clean default-output device change.
struct SystemAudioMonitor {
    latch: Latch,
    _shutdown_tx: mpsc::Sender<()>,
}

impl SystemAudioMonitor {
    fn new(
        aggregate_device_id: AudioDeviceID,
        tap_id: AudioObjectID,
        stream_weak: Weak<Mutex<StreamInner>>,
        error_callback: Arc<Mutex<ErrorCallback>>,
    ) -> Result<Self, Error> {
        let (event_rx, shutdown_tx) =
            spawn_system_audio_monitor_thread(aggregate_device_id, tap_id)?;

        let mut latch = Latch::new();
        let waiter = latch.waiter();

        let handle = std::thread::Builder::new()
            .name("cpal-coreaudio-system-audio".into())
            .spawn(move || {
                if !waiter.wait() {
                    return;
                }
                while let Ok(event) = event_rx.recv() {
                    let Some(arc) = stream_weak.upgrade() else {
                        break;
                    };

                    if let Ok(mut inner) = arc.try_lock() {
                        let _ = inner.pause();
                    };
                    let err = match event {
                        SystemAudioMonitorEvent::DeviceNotAvailable(message) => {
                            Error::with_message(ErrorKind::DeviceNotAvailable, message)
                        }
                        SystemAudioMonitorEvent::StreamInvalidated(message) => {
                            Error::with_message(ErrorKind::StreamInvalidated, message)
                        }
                    };
                    emit_error(&error_callback, err);
                }
            })
            .map_err(|e| {
                Error::with_message(
                    ErrorKind::ResourceExhausted,
                    format!("failed to spawn system-audio monitor thread: {e}"),
                )
            })?;

        latch.add_thread(handle.thread().clone());
        Ok(Self {
            latch,
            _shutdown_tx: shutdown_tx,
        })
    }
}

impl Monitor for SystemAudioMonitor {
    fn signal_ready(&self) {
        self.latch.release();
    }
}

struct StreamInner {
    playing: bool,
    audio_unit: AudioUnit,
    // Track the device with which the audio unit was spawned
    _device_id: AudioDeviceID,
    /// Manage the lifetime of the aggregate device used for loopback recording
    _loopback_device: Option<LoopbackDevice>,
}

impl StreamInner {
    fn play(&mut self) -> Result<(), Error> {
        if !self.playing {
            self.audio_unit
                .start()
                .context("Failed to start audio unit")?;
            self.playing = true;
        }
        Ok(())
    }

    fn pause(&mut self) -> Result<(), Error> {
        if self.playing {
            self.audio_unit
                .stop()
                .context("Failed to stop audio unit")?;
            self.playing = false;
        }
        Ok(())
    }
}

pub struct Stream {
    inner: Arc<Mutex<StreamInner>>,
    monitor: Box<dyn Monitor>,
}

impl Stream {
    fn new(inner: Arc<Mutex<StreamInner>>, monitor: Box<dyn Monitor>) -> Self {
        Self { inner, monitor }
    }

    fn signal_ready(&self) {
        self.monitor.signal_ready();
    }
}

impl Drop for Stream {
    fn drop(&mut self) {
        // Unblock monitor delivery threads if the stream is dropped early.
        self.monitor.signal_ready();
    }
}

impl StreamTrait for Stream {
    fn play(&self) -> Result<(), Error> {
        self.inner
            .lock()
            .map_err(|_| Error::with_message(ErrorKind::StreamInvalidated, "Stream lock poisoned"))?
            .play()
    }

    fn pause(&self) -> Result<(), Error> {
        self.inner
            .lock()
            .map_err(|_| Error::with_message(ErrorKind::StreamInvalidated, "Stream lock poisoned"))?
            .pause()
    }

    fn now(&self) -> StreamInstant {
        let m_host_time = unsafe { mach2::mach_time::mach_absolute_time() };
        host_time_to_stream_instant(m_host_time).expect("mach_timebase_info failed")
    }

    fn buffer_size(&self) -> Result<FrameCount, Error> {
        let stream = self.inner.lock().map_err(|_| {
            Error::with_message(ErrorKind::StreamInvalidated, "Stream lock poisoned")
        })?;
        device::get_device_buffer_frame_size(&stream.audio_unit)
            .map(|size| size as FrameCount)
            .context("Failed to get buffer frame size")
    }
}

#[cfg(test)]
mod test {
    use crate::{
        default_host,
        traits::{DeviceTrait, HostTrait, StreamTrait},
        InputCallbackInfo, OutputCallbackInfo, Sample,
    };

    #[test]
    fn test_play() {
        let host = default_host();
        let device = host.default_output_device().unwrap();

        let mut supported_configs_range = device.supported_output_configs().unwrap();
        let supported_config = supported_configs_range
            .next()
            .unwrap()
            .with_max_sample_rate();
        let config = supported_config.config();

        let stream = device
            .build_output_stream(
                config,
                write_silence::<f32>,
                move |err| println!("Error: {err}"),
                None, // None=blocking, Some(Duration)=timeout
            )
            .unwrap();
        stream.play().unwrap();
        std::thread::sleep(std::time::Duration::from_secs(1));
    }

    #[test]
    fn test_record() {
        let host = default_host();
        let device = host.default_input_device().unwrap();
        println!("Device: {:?}", device.description());

        let mut supported_configs_range = device.supported_input_configs().unwrap();
        println!("Supported configs:");
        for config in supported_configs_range.clone() {
            println!("{:?}", config)
        }
        let supported_config = supported_configs_range
            .next()
            .unwrap()
            .with_max_sample_rate();
        let config = supported_config.config();

        let stream = device
            .build_input_stream(
                config,
                move |data: &[f32], _: &InputCallbackInfo| {
                    // react to stream events and read or write stream data here.
                    println!("Got data: {:?}", &data[..25]);
                },
                move |err| println!("Error: {err}"),
                None, // None=blocking, Some(Duration)=timeout
            )
            .unwrap();
        stream.play().unwrap();
        std::thread::sleep(std::time::Duration::from_secs(1));
    }

    #[test]
    fn test_record_output() {
        if std::env::var("CI").is_ok() {
            println!("Skipping test_record_output in CI environment due to permissions");
            return;
        }

        let host = default_host();
        let device = host.default_output_device().unwrap();

        let mut supported_configs_range = device.supported_output_configs().unwrap();
        let supported_config = supported_configs_range
            .next()
            .unwrap()
            .with_max_sample_rate();
        let config = supported_config.config();

        println!("Building input stream");
        let stream = device
            .build_input_stream(
                config,
                move |data: &[f32], _: &InputCallbackInfo| {
                    // react to stream events and read or write stream data here.
                    println!("Got data: {:?}", &data[..25]);
                },
                move |err| println!("Error: {err}"),
                None, // None=blocking, Some(Duration)=timeout
            )
            .unwrap();
        stream.play().unwrap();
        std::thread::sleep(std::time::Duration::from_secs(1));
    }

    fn write_silence<T: Sample>(data: &mut [T], _: &OutputCallbackInfo) {
        for sample in data.iter_mut() {
            *sample = Sample::EQUILIBRIUM;
        }
    }
}
