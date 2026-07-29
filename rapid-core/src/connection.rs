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

pub struct Init {}

pub struct Authorised {}

pub struct Connection<S, P, AH, const TX_BUFF_SIZE: usize, const RX_BUFF_SIZE: usize>
where
    P: DeviceProtocol,
    AH: DeviceActionHandler<Protocol = P>,
{
    state: S,
    action_handler: PhantomData<AH>,
    protocol: PhantomData<P>,
    frame_assembler: FrameAssembler<'static>,
    /// Buffer should not be touched in sequence of polling, new message is allowed to be pulled only after the current returned reassembled message is dropped.
    tx: StreamProducer<&'static Buffer<TX_BUFF_SIZE>>,
    rx: StreamConsumer<&'static Buffer<RX_BUFF_SIZE>>, // <- add braces here
}

impl<S, P, AH, const TX_BUFF_SIZE: usize, const RX_BUFF_SIZE: usize>
    Connection<S, P, AH, TX_BUFF_SIZE, RX_BUFF_SIZE>
where
    P: DeviceProtocol,
    AH: DeviceActionHandler<Protocol = P>,
{
    /// Polls the next frame from rx buffer
    ///
    /// # Returns
    /// `None` if message was not assembled yet and needs more frames
    /// `Some(message_len)` if message was assembled
    pub async fn poll_frame(&mut self) -> Option<usize> {
        let grant = self.rx.wait_read().await;
        match self.frame_assembler.assemble(&grant) {
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
        let mut grant = self.tx.wait_grant_exact(TX_BUFF_SIZE).await;

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

    fn with_state<NS>(self, state: NS) -> Connection<NS, P, AH, TX_BUFF_SIZE, RX_BUFF_SIZE> {
        Connection {
            state,
            action_handler: self.action_handler,
            protocol: self.protocol,
            frame_assembler: self.frame_assembler,
            tx: self.tx,
            rx: self.rx,
        }
    }
}

impl<P, AH, const TX_BUFF_SIZE: usize, const RX_BUFF_SIZE: usize>
    Connection<Init, P, AH, TX_BUFF_SIZE, RX_BUFF_SIZE>
where
    P: DeviceProtocol,
    AH: DeviceActionHandler<Protocol = P>,
{
    pub async fn authorise(self) -> Connection<Authorised, P, AH, TX_BUFF_SIZE, RX_BUFF_SIZE> {
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
pub struct ConnectionModule<T: DeviceProtocol, ActionHandler: DeviceActionHandler<Protocol = T>> {
    handler: ActionHandler,
}

impl<T: DeviceProtocol, ActionHandler: DeviceActionHandler<Protocol = T>> Runnable
    for ConnectionModule<T, ActionHandler>
{
    async fn run(self) -> ! {
        loop {
            
        }
    }
}
