#![no_std]

use core::{marker::PhantomData, ops::Deref};

use rapid_types::Runnable;
use talky::device::{DeviceActionHandler, DeviceProtocol};

use crate::{
    bluetooth::BluetoothHandle, mqtt::MqttModule, storage::SequantialStorageModule,
    wifi::WifiModule,
};
pub mod bluetooth;
pub mod storage;
pub mod wifi;

mod mqtt;

struct RapidFramework<A, P>
where
    P: DeviceProtocol,
    A: DeviceActionHandler<P> + Runnable,
{
    bluetooth: BluetoothHandle,
    storage: SequantialStorageModule,
    wifi: WifiModule,
    mqtt: MqttModule,
    //Make this runnable
    device_app: A,
    protocol: PhantomData<P>,
}

impl<A, P> Deref for RapidFramework<A, P>
where
    P: DeviceProtocol,
    A: DeviceActionHandler<P> + Runnable,
{
    type Target = A;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.device_app
    }
}

impl<A, P> RapidFramework<A, P>
where
    P: DeviceProtocol,
    A: DeviceActionHandler<P> + Runnable,
{
    fn new(
        bluetooth: BluetoothHandle,
        storage: SequantialStorageModule,
        wifi: WifiModule,
        mqtt: MqttModule,
        device_app: A,
    ) -> Self {
        Self {
            bluetooth,
            storage,
            wifi,
            mqtt,
            device_app,
            protocol: PhantomData,
        }
    }

    fn run(&self) {}
}

#[cfg(test)]
pub mod tests {

    use super::*;

    #[tokio::test]
    async fn ss() {}
}
