use bbqueue::export::ConstInit;
use bbqueue::nicknames::Memphis;
use bbqueue::prod_cons::framed::FramedConsumer;
use bbqueue::prod_cons::stream::{StreamConsumer, StreamProducer};
use bbqueue::traits::notifier::{AsyncNotifier, Notifier};
use defmt::Debug2Format;
use embassy_time::Timer;
use rapid_macros::{actor, actor_handle};
use rapid_types::Runnable;
use serde::{Deserialize, Serialize};
use static_cell::StaticCell;
use talky::commands::action::ActionCommand;
use talky::commands::ping::{PingCommand, PingResponse};
use talky::commands::{CommandID, DeviceError};
use talky::framer::FrameAssembler;
use talky::{
    CommandRequest, Headers, Message, MessageHeaders, MessagePayload, MessageType, MessageV2,
};

use core::future::poll_fn;
use core::marker::PhantomData;
use core::pin::{Pin, pin};
use core::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use core::task::{Context, Poll};
use embassy_futures::select::{Either, Either3, select, select3};
use embassy_sync::blocking_mutex::raw::CriticalSectionRawMutex;
use embassy_sync::signal::Signal;
use embassy_sync::waitqueue::AtomicWaker;
use embassy_sync::watch::{self, Watch};
use embedded_storage::nor_flash::NorFlash;
use talky::commands::Command;
use talky::device::{DeviceActionHandler, DeviceProtocol};
use talky::types::error::ErrorWithKind;

const CONNECTION_TTL: u64 = 30000;

const LATCH_COUNT: usize = 1;

type Buffer<const Size: usize> = Memphis<Size, BufferNotifier>;

pub struct BufferNotifier {
    consumer_waker: AtomicWaker,
    producer_waker: AtomicWaker,
}

impl ConstInit for BufferNotifier {
    const INIT: Self = Self {
        consumer_waker: AtomicWaker::new(),
        producer_waker: AtomicWaker::new(),
    };
}

impl Notifier for BufferNotifier {
    fn wake_one_consumer(&self) {
        self.consumer_waker.wake();
    }
    fn wake_one_producer(&self) {
        self.producer_waker.wake();
    }
}

impl AsyncNotifier for BufferNotifier {
    async fn wait_for_not_empty<T, F: FnMut() -> Option<T>>(&self, mut f: F) -> T {
        poll_fn(|cx| {
            // register BEFORE checking — this is what closes the race:
            // if a wake happens between our check and next poll, it's not lost
            self.consumer_waker.register(cx.waker());
            match f() {
                Some(t) => Poll::Ready(t),
                None => Poll::Pending,
            }
        })
        .await
    }

    async fn wait_for_not_full<T, F: FnMut() -> Option<T>>(&self, mut f: F) -> T {
        poll_fn(|cx| {
            self.producer_waker.register(cx.waker());
            match f() {
                Some(t) => Poll::Ready(t),
                None => Poll::Pending,
            }
        })
        .await
    }
}

pub struct Latch {
    _grant: SlotGrant,
    receiver: watch::Receiver<'static, CriticalSectionRawMutex, (), LATCH_COUNT>,
}

impl Latch {
    pub fn new(
        receiver: watch::Receiver<'static, CriticalSectionRawMutex, (), LATCH_COUNT>,
        grant: SlotGrant,
    ) -> Self {
        Self {
            _grant: grant,
            receiver,
        }
    }

    pub async fn wait_until_closed(mut self) {
        self.receiver.changed().await;
    }
}

pub struct TransportIO<P: DeviceProtocol>
where
    [u8; P::OUTGOING_PAYLOAD_SIZE]: Sized + 'static,
    [u8; P::INCOMING_PAYLOAD_SIZE]: Sized + 'static,
{
    pub tx: StreamProducer<&'static Buffer<{ P::INCOMING_PAYLOAD_SIZE }>>,
    pub rx: StreamConsumer<&'static Buffer<{ P::OUTGOING_PAYLOAD_SIZE }>>,
    pub latch: Latch,
    _grant: SlotGrant,
}
pub struct ConnectionIO<P: DeviceProtocol>
where
    [u8; P::OUTGOING_PAYLOAD_SIZE]: Sized + 'static,
    [u8; P::INCOMING_PAYLOAD_SIZE]: Sized + 'static,
{
    pub tx: StreamProducer<&'static Buffer<{ P::OUTGOING_PAYLOAD_SIZE }>>,
    pub rx: StreamConsumer<&'static Buffer<{ P::INCOMING_PAYLOAD_SIZE }>>,
    pub assembler: &'static mut FrameAssembler<{ P::INCOMING_PAYLOAD_SIZE }>,
    _grant: SlotGrant,
}

/// Guard that guarantees that only one connection would be able to use it at a time
/// by creating `SlotGrant` instances.
pub struct SlotGuard {
    grants: AtomicUsize,
    acquired: AtomicBool,
}

impl SlotGuard {
    /// Creates a new slot guard.
    pub const fn new() -> Self {
        Self {
            grants: AtomicUsize::new(0),
            acquired: AtomicBool::new(false),
        }
    }

    /// Tries to create a slot grant from this guard.
    /// Returns `None` if the slot is already acquired.
    pub fn try_acquire(&'static self) -> Option<SlotGrant> {
        self.acquired
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .ok()?;
        Some(SlotGrant::new(&self.grants, &self.acquired))
    }
}
/// Grant that is created by a [`SlotGuard`] and is used as a token for using slot fields, whenever the `grants`
/// counter is non-zero it means that something from slot is still being used.
pub struct SlotGrant {
    grants: &'static AtomicUsize,
    acquired: &'static AtomicBool,
}

impl SlotGrant {
    fn new(grants: &'static AtomicUsize, acquired: &'static AtomicBool) -> Self {
        grants.fetch_add(1, Ordering::AcqRel);
        Self { grants, acquired }
    }
}

impl Clone for SlotGrant {
    fn clone(&self) -> Self {
        SlotGrant::new(self.grants, self.acquired)
    }
}

impl Drop for SlotGrant {
    fn drop(&mut self) {
        // If this is the last grant, release the guard
        if self.grants.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.acquired.store(false, Ordering::Release);
        }
    }
}
/// Structure that holds resources for exactly one connection.
///
/// NOTE: If multiple connections would somehow gain access to a single slot, it is UB.
pub struct ConnectionSlot<P>
where
    P: DeviceProtocol,
    [u8; P::OUTGOING_PAYLOAD_SIZE]: Sized + 'static,
    [u8; P::INCOMING_PAYLOAD_SIZE]: Sized + 'static,
{
    slot_guard: SlotGuard, //Ensure exclusive access to slot
    frame_assembler: UnsafeCell<FrameAssembler<{ P::INCOMING_PAYLOAD_SIZE }>>, //Used to assemble incoming messages
    tx: Buffer<{ P::OUTGOING_PAYLOAD_SIZE }>, //From connection to transport
    rx: Buffer<{ P::INCOMING_PAYLOAD_SIZE }>, //From transport to connection
    watch: Watch<CriticalSectionRawMutex, (), LATCH_COUNT>, //Used to notify transport that connection has died
}

impl<P> ConnectionSlot<P>
where
    P: DeviceProtocol,
    [u8; P::OUTGOING_PAYLOAD_SIZE]: Sized + 'static,
    [u8; P::INCOMING_PAYLOAD_SIZE]: Sized + 'static,
{
    /// Creates a new connection slot.
    pub const fn new() -> Self {
        Self {
            frame_assembler: UnsafeCell::new(FrameAssembler::new()),
            tx: Buffer::new(),
            rx: Buffer::new(),
            watch: Watch::new(),
            slot_guard: SlotGuard::new(),
        }
    }

    /// Resets the connection slot, notifying the transport that the connection has died.
    pub fn reset(&self) {
        let _ = self.watch.sender().send(());
    }

    /// Tries to acquire a slot grant from this connection slot.
    /// Returns `None` if the slot is already acquired.
    pub fn try_aquire(&'static self) -> Option<(ConnectionIO<P>, TransportIO<P>)> {
        let Some(grant) = self.slot_guard.try_acquire() else {
            defmt::warn!("[ConnectionSlot] Failed to aquire slot guard");
            return None;
        };

        let Some(receiver) = self.watch.receiver() else {
            // Realistically, this should never happen, as the slot guard is already acquired.
            defmt::error!(
                "[ConnectionSlot] Failed to aquire receiver, although grant was successfully acquired"
            );
            return None;
        };
        Some((
            ConnectionIO {
                tx: self.tx.stream_producer(),
                rx: self.rx.stream_consumer(),
                // SAFETY: Slot guard guarantees that if its grant counter is 0, the previous instance aquirment was fully dropped
                assembler: unsafe { self.frame_assembler.as_mut_unchecked() },
                _grant: grant.clone(),
            },
            TransportIO {
                tx: self.rx.stream_producer(),
                rx: self.tx.stream_consumer(),
                latch: Latch::new(receiver, grant.clone()),
                _grant: grant,
            },
        ))
    }
}

pub struct Init {}

pub struct Authorised {}

pub struct Connection<S, P, AH>
where
    P: DeviceProtocol,
    AH: DeviceActionHandler<Protocol = P>,
    [u8; P::OUTGOING_PAYLOAD_SIZE]: Sized + 'static,
    [u8; P::INCOMING_PAYLOAD_SIZE]: Sized + 'static,
{
    state: S,
    action_handler: PhantomData<AH>,
    protocol: PhantomData<P>,
    io: ConnectionIO<P>,
}

impl<S, P, AH> Connection<S, P, AH>
where
    P: DeviceProtocol,
    AH: DeviceActionHandler<Protocol = P>,
    [u8; P::OUTGOING_PAYLOAD_SIZE]: Sized + 'static,
    [u8; P::INCOMING_PAYLOAD_SIZE]: Sized + 'static,
{
    pub fn new(state: S, io: ConnectionIO<P>) -> Self {
        Self {
            state,
            action_handler: PhantomData,
            protocol: PhantomData,
            io,
        }
    }

    /// Polls the next frame from rx buffer
    ///
    /// # Returns
    /// `None` if message was not assembled yet and needs more frames
    /// `Some(message_len)` if message was assembled
    pub async fn poll_frame(&mut self) -> Option<usize> {
        let grant = self.io.rx.wait_read().await;
        match self.io.assembler.assemble(&grant) {
            Ok(message_len) => message_len,
            Err(e) => {
                defmt::error!(
                    "[Connection] Failed to assemble frame: {}",
                    Debug2Format(&e)
                );
                None
            }
        }
    }

    /// Polls frames from the rx buffer to assemble a message
    ///
    /// # Returns
    /// `None` if `CONNECTION_TTL` was reached before assembling the message
    /// `Some(message_len)` if message was assembled successfully
    async fn poll_message(&mut self) -> Option<usize> {
        let assemble_message = async {
            loop {
                if let Some(len) = self.poll_frame().await {
                    defmt::trace!("[Connection] Assembled message with length: {}", len);
                    return len;
                }
                defmt::trace!("[Connection] Waiting for next frame");
            }
        };
        match select(assemble_message, Timer::after_millis(CONNECTION_TTL)).await {
            // The hardware/socket provided a result
            Either::First(message_len) => Some(message_len),
            // The timer reached CONNECTION_TTL first
            Either::Second(_) => {
                defmt::warn!(
                    "[Connection] TTL Expired after while polling for frame {}ms.",
                    CONNECTION_TTL
                );
                None
            }
        }
    }

    pub async fn run(mut self, handler: AH) {
        loop {
            let message_len = self.poll_message().await;
            let Some(message_len) = message_len else {
                defmt::warn!(
                    "[Connection] Received None when polling for message, clossing connection."
                );
                break;
            };

            let message = self.frame_assembler.get_from_buffer(message_len);

            let headers = serde_json_core::from_slice::<MessageHeaders>(message).map(|v| v.0);
            let Ok(headers) = headers else {
                defmt::warn!(
                    "[Connection] Failed to parse message headers: {}",
                    Debug2Format(&headers.err())
                );
                continue;
            };

            match headers.message_type {
                MessageType::Request => self.handle_request(&headers, &message, &handler).await,
                MessageType::Response => {
                    defmt::warn!(
                        "[Connection] Received unsolicited response: {}",
                        Debug2Format(&message)
                    );
                    continue;
                }
            };
        }
    }

    pub async fn handle_request(&self, headers: &Headers, message: &[u8], handler: &AH) {
        let mut grant = self.io.tx.wait_grant_exact(P::OUTGOING_PAYLOAD_SIZE).await;

        let result: Result<usize, DeviceError> = async {
            match headers.id {
                CommandID::Ping => {
                    let response: <PingCommand as Command>::Response = PingResponse;
                    self.respond::<PingCommand>(Ok(response), headers, &mut grant)
                }
                CommandID::Action => {
                    let action: <ActionCommand<P> as Command>::Request =
                        self.decode_payload(message)?;
                    let response = handler.handle_action(action).await;
                    self.respond::<ActionCommand<P>>(
                        response.map_err(DeviceError::from),
                        headers,
                        &mut grant,
                    )
                }
                _ => Ok(0),
            }
        }
        .await;

        match result {
            Ok(bytes) if bytes > 0 => {
                grant.commit(bytes);
            }
            Err(e) => {
                defmt::error!("[Connection] Error handling request: {}", Debug2Format(&e));
            }
            _ => {
                defmt::warn!(
                    "[Connection] Unexpected len of result: {:?}",
                    Debug2Format(&result)
                );
            }
        }
    }

    fn respond<C: Command>(
        &self,
        result: Result<C::Response, DeviceError>,
        headers: &Headers,
        grant: &mut [u8],
    ) -> Result<usize, DeviceError> {
        let message = talky::response::<C>(result, headers.timestamp);
        self.encode(&message, grant)
    }

    fn decode_payload<'a, T: Deserialize<'a>>(&self, bytes: &'a [u8]) -> Result<T, DeviceError> {
        self.decode::<MessagePayload<T>>(bytes).map(|v| v.payload)
    }

    fn decode<'a, T: Deserialize<'a>>(&self, bytes: &'a [u8]) -> Result<T, DeviceError> {
        serde_json_core::from_slice(bytes)
            .map_err(DeviceError::from_serde_core_de)
            .map(|v| v.0)
    }

    fn encode<T: Serialize>(&self, value: &T, buf: &mut [u8]) -> Result<usize, DeviceError> {
        serde_json_core::to_slice(value, buf).map_err(DeviceError::from_serde_core_ser)
    }

    fn with_state<NS>(self, state: NS) -> Connection<NS, P, AH> {
        Connection {
            state,
            action_handler: self.action_handler,
            protocol: self.protocol,
            io: self.io,
        }
    }
}

impl<P, AH> Connection<Init, P, AH>
where
    P: DeviceProtocol,
    AH: DeviceActionHandler<Protocol = P>,
    [u8; P::OUTGOING_PAYLOAD_SIZE]: Sized + 'static,
    [u8; P::INCOMING_PAYLOAD_SIZE]: Sized + 'static,
{
    pub async fn authorise(self) -> Connection<Authorised, P, AH> {
        // let result = 'scoped: {
        //            let mut inner = self.inner.lock().await;
        //            let Some(request) = self.poll_with_timeout().await else {
        //                info!("[Conn] Timed out waiting for handshake request");
        //                break 'scoped false;
        //            };

        //            let Some(session) = self.try_handle_handshake::<S>(&request).await else {
        //                log::warn!("[Conn] Handshake failed for request ID: {}", request.id);
        //                break 'scoped false;
        //            };
        //            log::info!("[Conn] Handshake passed, authorization successful");

        //            inner.state = ConnectionState::Authorized(session);
        //            true
        //        };
        self.with_state(Authorised {})
    }
}

#[actor_handle(ConnectionHandle)]
pub trait ConnectionModuleHandle {
    async fn request_connection(&self);
}

#[actor(ConnectionHandle)]
pub struct ConnectionModule<Protocol, ActionHandler>
where
    Protocol: DeviceProtocol,
    ActionHandler: DeviceActionHandler<Protocol = Protocol>,
    [u8; Protocol::OUTGOING_PAYLOAD_SIZE]: Sized + 'static,
    [u8; Protocol::INCOMING_PAYLOAD_SIZE]: Sized + 'static,
{
    slots: &'static [ConnectionSlot<Protocol>; 2],
    handler: ActionHandler,
}

impl<Protocol, ActionHandler> Runnable for ConnectionModule<Protocol, ActionHandler>
where
    Protocol: DeviceProtocol,
    ActionHandler: DeviceActionHandler<Protocol = Protocol>,
    [u8; Protocol::OUTGOING_PAYLOAD_SIZE]: Sized + 'static,
    [u8; Protocol::INCOMING_PAYLOAD_SIZE]: Sized + 'static,
{
    async fn run(self) -> ! {
        let slot1 = &self.slots[0];
        let slot2 = &self.slots[1];
        
        loop {}
    }
}

impl<Protocol, ActionHandler> ConnectionModule<Protocol, ActionHandler>
where
    Protocol: DeviceProtocol,
    ActionHandler: DeviceActionHandler<Protocol = Protocol>,
    [u8; Protocol::OUTGOING_PAYLOAD_SIZE]: Sized + 'static,
    [u8; Protocol::INCOMING_PAYLOAD_SIZE]: Sized + 'static,
{
    pub fn new(handler: ActionHandler, slots: &'static [ConnectionSlot<Protocol>; 2]) -> Self {
        Self { slots, handler }
    }
}
