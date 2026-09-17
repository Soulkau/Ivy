use core::{cell::RefCell, ops::Range};

use const_panic::concat_panic;
use embassy_embedded_hal::adapter::BlockingAsync;
use embassy_sync::blocking_mutex::{Mutex as BlockingMutex, raw::CriticalSectionRawMutex};
use embassy_sync::mutex::Mutex as AsyncMutex;
use embedded_storage::nor_flash::NorFlash;
use embedded_storage::nor_flash::{ErrorType, ReadNorFlash};
use sequential_storage::cache::NoCache;
use sequential_storage::map::{MapConfig, MapStorage};
use serde::{Deserialize, Serialize};

type SynchronizedFlash<F> = BlockingMutex<CriticalSectionRawMutex, RefCell<F>>;

pub struct IvyFlash<F: NorFlash + 'static> {
    flash: &'static SynchronizedFlash<F>,
}

impl<F: NorFlash> IvyFlash<F> {
    pub fn new(flash: &'static SynchronizedFlash<F>) -> Self {
        Self { flash }
    }

    pub fn partition(&self, range: Range<u32>) -> IvyFlashPartition<F> {
        IvyFlashPartition::new(self.flash, range)
    }
}

pub struct IvyFlashPartition<F: NorFlash + 'static> {
    flash: &'static SynchronizedFlash<F>,
    range: Range<u32>, // start = offset, end = offset + size
}

impl<F: NorFlash> IvyFlashPartition<F> {
    pub const fn new(flash: &'static SynchronizedFlash<F>, range: Range<u32>) -> Self {
        Self { flash, range }
    }

    fn offset(&self) -> u32 {
        self.range.start
    }

    fn size(&self) -> u32 {
        self.range.end - self.range.start
    }
}

impl<F: NorFlash> ErrorType for IvyFlashPartition<F> {
    type Error = F::Error;
}

impl<F: NorFlash> ReadNorFlash for IvyFlashPartition<F> {
    const READ_SIZE: usize = F::READ_SIZE;

    fn read(&mut self, off: u32, buf: &mut [u8]) -> Result<(), Self::Error> {
        let offset = self.offset();
        self.flash.lock(|cell| cell.borrow_mut().read(offset + off, buf))
    }

    fn capacity(&self) -> usize {
        self.size() as usize
    }
}

impl<F: NorFlash> NorFlash for IvyFlashPartition<F> {
    const WRITE_SIZE: usize = F::WRITE_SIZE;
    const ERASE_SIZE: usize = F::ERASE_SIZE;

    fn erase(&mut self, from: u32, to: u32) -> Result<(), Self::Error> {
        let offset = self.offset();
        self.flash.lock(|cell| cell.borrow_mut().erase(offset + from, offset + to))
    }

    fn write(&mut self, off: u32, data: &[u8]) -> Result<(), Self::Error> {
        let offset = self.offset();
        self.flash.lock(|cell| cell.borrow_mut().write(offset + off, data))
    }
}

#[macro_export]
macro_rules! init_flash {
    ($flash_ty:ty, $flash:expr) => {{
        static CELL: static_cell::StaticCell<embassy_sync::blocking_mutex::Mutex<embassy_sync::blocking_mutex::raw::CriticalSectionRawMutex, core::cell::RefCell<$flash_ty>>> =
            static_cell::StaticCell::new();
        let flash_ref = CELL.init_with(|| embassy_sync::blocking_mutex::Mutex::new(core::cell::RefCell::new($flash)));
        $crate::storage::IvyFlash::<$flash_ty>::new(flash_ref)
    }};
}

pub struct StorageInner<F: NorFlash + 'static> {
    storage: MapStorage<u32, BlockingAsync<IvyFlashPartition<F>>, NoCache>,
    ser_buf: [u8; 256],
    work_buf: [u8; 256],
}

pub type Inner<F> = AsyncMutex<CriticalSectionRawMutex, StorageInner<F>>;

pub struct FlashStorage<F: NorFlash + 'static> {
    pub inner: &'static Inner<F>,
}

impl<F: NorFlash> FlashStorage<F> {
    #[doc(hidden)]
    pub fn build(partition: IvyFlashPartition<F>) -> Inner<F> {
        let map_config = MapConfig::new(0..partition.size()); // relative, not absolute
        AsyncMutex::new(StorageInner {
            storage: MapStorage::new(BlockingAsync::new(partition), map_config, NoCache::new()),
            ser_buf: [0u8; 256],
            work_buf: [0u8; 256],
        })
    }

    pub fn from_static(inner: &'static Inner<F>) -> Self {
        Self { inner }
    }

    pub async fn get<D: for<'de> Deserialize<'de>>(&self, key: StorageKey) -> Option<D> {
        let mut guard = self.inner.lock().await;
        let inner = &mut *guard;
        let item_data = inner.storage.fetch_item(&mut inner.ser_buf, key.as_ref()).await.ok()??;
        let data: D = postcard::from_bytes(item_data).ok()?;
        Some(data)
    }

    pub async fn set<S: Serialize>(&self, key: StorageKey, value: &S) {
        let mut guard = self.inner.lock().await;
        let inner = &mut *guard;
        let serialized: &[u8] = postcard::to_slice(value, &mut inner.ser_buf).unwrap();
        let _ = inner.storage.store_item(&mut inner.work_buf, &key.as_ref(), &serialized).await;
    }
}

#[macro_export]
macro_rules! init_storage {
    ($flash_ty:ty, $partition:expr) => {{
        static CELL: static_cell::StaticCell<$crate::storage::Inner<$flash_ty>> = static_cell::StaticCell::new();
        let storage_ref = CELL.init_with(|| $crate::storage::StorageModule::<$flash_ty>::build($partition));
        $crate::storage::StorageModule::from_static(storage_ref)
    }};
}

pub struct StorageKey(pub(crate) u32);

pub const MIN_KEY_VALUE: u32 = 100;

impl StorageKey {
    pub const fn new(key: u32) -> Self {
        if key < MIN_KEY_VALUE {
            concat_panic!("This ket is reserved for system use: ", key);
        }
        Self(key)
    }

    pub(crate) const fn metadata_key() -> Self {
        Self(1)
    }
}

impl AsRef<u32> for StorageKey {
    fn as_ref(&self) -> &u32 {
        &self.0
    }
}
