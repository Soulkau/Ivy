use embassy_futures::select::{Either, select};
use heapless::Vec as HVec;
use ivy_macros::actor_handle;
use ivy_types::actor::*;
use talky::bluetooth::DeviceAdvertisimentInfo;
use trouble_host::{Address, Controller, Host, HostResources, prelude::*};

use crate::device::DeviceMetadata;

//0000ca7e-0000-1000-8000-00805f9b34fb
const RAW_SERVICE_UUID: [u8; 2] = [0x7e, 0xca];

const MAX_MTU: usize = 256;

#[actor_handle(BluetoothHandle)]
trait BluetoothHandleTrait {
    async fn start_advertising(&self);
    async fn stop_advertising(&self);
}

pub struct BluetoothModule<C: Controller + 'static> {
    controller: Option<C>,
    device_meta: &'static DeviceMetadata,
}

impl<C: Controller + 'static> BluetoothModule<C> {
    pub fn new(controller: C, device_meta: &'static DeviceMetadata) -> Self {
        Self {
            controller: Some(controller),
            device_meta,
        }
    }
}

impl<C: Controller + 'static> Actor for BluetoothModule<C> {
    type Handle = BluetoothHandle;

    async fn act(&mut self, mut inbox: Inbox<<Self::Handle as ActorHandle>::Cmd>) -> ! {
        let mut resources: HostResources<DefaultPacketPool, 1, 1> = HostResources::new();
        //TODO: Get a real address generation
        let stack = trouble_host::new(self.controller.take().expect("Failed to take controller"), &mut resources).set_random_address(Address::random([22, 111, 251, 222, 45, 55]));

        let Host { mut peripheral, runner, .. } = stack.build();

        let server = DeviceServer::new_with_config(GapConfig::Peripheral(PeripheralConfig {
            name: self.device_meta.name.as_str(),
            appearance: &appearance::UNKNOWN,
        }))
        .unwrap();

        let device_info = DeviceAdvertisimentInfo::new(self.device_meta.device_type, self.device_meta.device_id.to_short()).to_bytes().unwrap();

        let mut advertiser_data = [0; 31];
        let len = AdStructure::encode_slice(
            &[
                AdStructure::Flags(LE_GENERAL_DISCOVERABLE | BR_EDR_NOT_SUPPORTED),
                AdStructure::ServiceUuids16(&[RAW_SERVICE_UUID]),
                AdStructure::CompleteLocalName(self.device_meta.name.as_bytes()),
                AdStructure::ManufacturerSpecificData {
                    company_identifier: talky::bluetooth::MANUFACTURER_ID,
                    payload: &device_info,
                },
            ],
            &mut advertiser_data[..],
        )
        .unwrap();
        let advirtisement = Advertisiment { advertiser_data, len };
        loop {
            let command = inbox.next();

            match select(command, async {}).await {
                Either::First(cmd) => match cmd {
                    BluetoothHandleTraitCommand::StartAdvertising(consumer) => {
                        consumer.ack().await;
                    }
                    BluetoothHandleTraitCommand::StopAdvertising(consumer) => {
                        consumer.ack().await;
                    }
                },
                Either::Second(_) => { /* reconnect logic */ }
            }
        }
    }
}

#[derive(Clone, Copy)]
struct Advertisiment {
    advertiser_data: [u8; 31],
    len: usize,
}

#[gatt_service(uuid = RAW_SERVICE_UUID)]
pub struct BluetoothService {
    #[characteristic(uuid = "0000ca72-0000-1000-8000-00805f9b34fb", write)]
    pub write: HVec<u8, MAX_MTU>,
    #[characteristic(uuid = "0000ca71-0000-1000-8000-00805f9b34fb", read, notify)]
    pub read: HVec<u8, MAX_MTU>,
}

#[gatt_server]
pub struct DeviceServer {
    pub service: BluetoothService,
}
