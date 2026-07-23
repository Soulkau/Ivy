use embassy_futures::select::{Either, select};
use embassy_sync::{blocking_mutex::raw::CriticalSectionRawMutex, channel::Channel};

use rapid_macros::{actor, actor_handle};
use rapid_types::Runnable;
use trouble_host::Controller;

#[actor_handle(BluetoothHandle)]
trait BluetoothHandleTrait {
    async fn start_advertising(&self);
    async fn stop_advertising(&self);
}

#[actor(BluetoothHandle)]
pub struct BluetoothModule<C: Controller + 'static> {
    controller: C,
}

impl<C: Controller> BluetoothModule<C> {
    pub fn new(c: C) -> Self {
        Self { controller: c }
    }
}

impl<C: Controller + 'static> Runnable for BluetoothModule<C> {
    async fn run(self) -> ! {
        loop {
            let command = BluetoothModule::<C>::next_command();
            
            match select(command, async {}).await {
                Either::First(cmd) => match cmd {
                    BluetoothHandleTraitCommand::StartAdvertising(consumer) => {
                        consumer.reply(());
                    }
                    BluetoothHandleTraitCommand::StopAdvertising(consumer) => {
                        consumer.reply(());
                    }
                },
                Either::Second(_) => { /* reconnect logic */ }
            }
        }
    }
}
