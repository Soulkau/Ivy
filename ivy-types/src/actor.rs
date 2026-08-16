use embassy_sync::{
    blocking_mutex::raw::NoopRawMutex,
    channel::{DynamicReceiver, DynamicSender, Sender},
};

pub trait ActorHandle: 'static {
    type Cmd: 'static;
    fn create(cmd_tx: ::embassy_sync::channel::DynamicSender<'static, Self::Cmd>) -> Self;
}

pub struct ReplyConsumer<T: 'static>(&'static DynamicSender<'static, T>);

impl<T> ReplyConsumer<T> {
    pub fn new(sender: &'static DynamicSender<'static, T>) -> Self {
        Self(sender)
    }

    pub async fn reply(&self, resp: T) {
        self.0.send(resp).await;
    }
}
impl ReplyConsumer<()> {
    pub async fn ack(&self) {
        self.0.send(()).await;
    }
}

pub type ActorSender<P, const QUEUE_SIZE: usize> = Sender<'static, NoopRawMutex, P, QUEUE_SIZE>;

pub struct Inbox<P: 'static> {
    receiver: DynamicReceiver<'static, P>,
}

impl<P: 'static> Inbox<P> {
    pub fn new(receiver: DynamicReceiver<'static, P>) -> Self {
        Self { receiver }
    }

    pub async fn next(&mut self) -> P {
        self.receiver.receive().await
    }
}

pub trait Actor: 'static {
    type Handle: ActorHandle;

    async fn act(&mut self, inbox: Inbox<<Self::Handle as ActorHandle>::Cmd>) -> !;
}

#[macro_export]
macro_rules! actor {
    // Form 1: Explicit actor type, actor instance, custom queue size
    ($spawner:expr, $ActorType:ty, $actor_expr:expr, $queue_size:expr) => {{
        type Cmd = <<$ActorType as $crate::actor::Actor>::Handle as $crate::actor::ActorHandle>::Cmd;

        // Static channel allocation
        static CHANNEL: ::static_cell::StaticCell<::embassy_sync::channel::Channel<::embassy_sync::blocking_mutex::raw::NoopRawMutex, Cmd, $queue_size>> = ::static_cell::StaticCell::new();
        let channel = CHANNEL.init(::embassy_sync::channel::Channel::new());

        // Static actor instance allocation
        static ACTOR: ::static_cell::StaticCell<$ActorType> = ::static_cell::StaticCell::new();
        let actor = ACTOR.init($actor_expr);

        // Task wrapper using the Actor::act method directly
        #[embassy_executor::task]
        async fn actor_task(actor: &'static mut $ActorType, receiver: ::embassy_sync::channel::DynamicReceiver<'static, Cmd>) -> ! {
            let inbox = $crate::actor::Inbox::new(receiver);
            actor.act(inbox).await
        }

        $spawner.spawn(actor_task(actor, channel.dyn_receiver())).unwrap();

        <<$ActorType as $crate::actor::Actor>::Handle as $crate::actor::ActorHandle>::create(channel.dyn_sender())
    }};

    // Form 2: Defaults to a queue size of 4
    ($spawner:expr, $ActorType:ty, $actor_expr:expr) => {
        $crate::actor!($spawner, $ActorType, $actor_expr, 4)
    };
}

pub mod rt {

    use embassy_sync::{
        blocking_mutex::raw::NoopRawMutex,
        channel::{Channel, DynamicSender},
    };

    use crate::actor::ReplyConsumer;

    #[must_use = "to delay the drop bomb invokation to the end of the scope"]
    pub struct DropBomb(());
    impl DropBomb {
        pub fn new() -> Self {
            Self(())
        }

        /// Defuses the bomb, rendering it safe to drop.
        pub fn defuse(self) {
            core::mem::forget(self)
        }
    }

    impl Drop for DropBomb {
        fn drop(&mut self) {
            panic!("Dropped before the request completed. You  cannot cancel an ongoing request")
        }
    }

    pub async fn request<Cmd, T: 'static>(actor_sender: &DynamicSender<'static, Cmd>, build: impl FnOnce(ReplyConsumer<T>) -> Cmd) -> T {
        let channel: Channel<NoopRawMutex, T, 1> = Channel::new();
        let sender: DynamicSender<'_, T> = channel.sender().into();
        let bomb = DropBomb::new();

        // We guarantee that channel lives until we've been notified on it, at which
        // point its out of reach for the replier.
        let reply_to = unsafe { core::mem::transmute::<&embassy_sync::channel::DynamicSender<'_, T>, &'static embassy_sync::channel::DynamicSender<'_, T>>(&sender) };

        let consumer = ReplyConsumer::new(reply_to);

        actor_sender.send(build(consumer)).await;
        let value = channel.receive().await;

        bomb.defuse();
        value
    }
}
