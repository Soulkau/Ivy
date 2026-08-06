#![allow(async_fn_in_trait)]
#![no_std]

use embassy_executor::Spawner;
use embassy_sync::{blocking_mutex::raw::CriticalSectionRawMutex, signal::Signal};

type ActorChannel<T> = embassy_sync::channel::Channel<CriticalSectionRawMutex, T, 4>;

pub trait Runnable: 'static {
    async fn run(self) -> !;
}

pub trait Actor: Runnable {
    type Handle: Send + Sync + 'static;
}

pub trait ActorHandle {
    type Command;
    type Signals;
}

pub struct ResponseConsumer<T: 'static>(pub &'static Signal<CriticalSectionRawMutex, T>);

impl<T> ResponseConsumer<T> {
    pub fn reply(&self, resp: T) {
        self.0.signal(resp);
    }
}
