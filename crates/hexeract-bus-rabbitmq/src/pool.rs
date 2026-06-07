use std::collections::VecDeque;

use hexeract_bus::BusError;
use lapin::Channel;
use lapin::options::ConfirmSelectOptions;
use tokio::sync::Mutex;

use crate::connection::RabbitMqConnection;

/// Default maximum number of idle channels cached per pool.
pub const DEFAULT_POOL_MAX_SIZE: usize = 8;

/// Small bounded pool of [`lapin::Channel`] handles for a single publisher.
///
/// A channel is cheap to clone (it wraps an `Arc` internally) but
/// opening one always round-trips to the broker. The pool keeps
/// already-opened channels around between publishes and tops up on
/// demand. Channels whose connection has dropped are discarded and a
/// fresh one is opened.
///
/// By default every channel the pool opens has publisher confirms
/// enabled (`confirm_select`), so publishes through pooled channels
/// can await a broker acknowledgement. Confirm mode is sticky for the
/// lifetime of an AMQP channel, so recycled channels keep it without
/// further negotiation. Opt out with [`Self::without_confirms`].
#[derive(Debug)]
pub struct ChannelPool {
    connection: RabbitMqConnection,
    idle: Mutex<VecDeque<Channel>>,
    max_size: usize,
    confirms: bool,
}

impl ChannelPool {
    /// Build a new pool backed by `connection`.
    ///
    /// `max_size` caps the number of idle channels cached between
    /// publishes. `0` is normalised to `1` so the pool always has room
    /// for at least one channel. Channels are opened with publisher
    /// confirms enabled.
    #[must_use]
    pub fn new(connection: RabbitMqConnection, max_size: usize) -> Self {
        let max_size = max_size.max(1);
        Self {
            connection,
            idle: Mutex::new(VecDeque::with_capacity(max_size)),
            max_size,
            confirms: true,
        }
    }

    /// Disable publisher confirms on channels this pool opens.
    ///
    /// Call before the first [`Self::acquire`]: confirm mode is sticky
    /// per AMQP channel, so channels already cached keep the mode they
    /// were opened with.
    #[must_use]
    pub fn without_confirms(mut self) -> Self {
        self.confirms = false;
        self
    }

    /// Whether channels opened by this pool have publisher confirms enabled.
    #[must_use]
    pub fn confirms(&self) -> bool {
        self.confirms
    }

    /// Borrow the underlying [`RabbitMqConnection`].
    #[must_use]
    pub fn connection(&self) -> &RabbitMqConnection {
        &self.connection
    }

    /// Maximum number of idle channels the pool retains.
    #[must_use]
    pub fn max_size(&self) -> usize {
        self.max_size
    }

    /// Acquire a channel, opening a fresh one if the cache is empty.
    ///
    /// A fresh channel is put in confirm mode before being handed out,
    /// unless the pool was built with [`Self::without_confirms`]. The
    /// returned [`PooledChannel`] returns the channel to the pool on
    /// drop unless the underlying connection is no longer usable.
    ///
    /// # Errors
    ///
    /// Returns [`BusError::Connection`] if no cached channel is
    /// available and opening or configuring a new one fails.
    pub async fn acquire(&self) -> Result<PooledChannel<'_>, BusError> {
        let cached = {
            let mut idle = self.idle.lock().await;
            let mut found = None;
            while let Some(channel) = idle.pop_front() {
                if channel.status().connected() {
                    found = Some(channel);
                    break;
                }
            }
            found
        };
        let channel = if let Some(channel) = cached {
            channel
        } else {
            let channel = self.connection.create_channel().await?;
            if self.confirms {
                channel
                    .confirm_select(ConfirmSelectOptions::default())
                    .await
                    .map_err(|err| BusError::Connection(Box::new(err)))?;
            }
            channel
        };
        Ok(PooledChannel {
            channel: Some(channel),
            pool: self,
        })
    }

    /// Number of idle channels currently cached. Useful for tests.
    #[must_use]
    pub async fn idle_len(&self) -> usize {
        self.idle.lock().await.len()
    }
}

/// RAII guard returned by [`ChannelPool::acquire`].
///
/// Returns the underlying channel to the pool when dropped, on a
/// best-effort basis: if the pool mutex is contended at drop time the
/// channel is simply released and the broker will close it.
#[derive(Debug)]
pub struct PooledChannel<'a> {
    channel: Option<Channel>,
    pool: &'a ChannelPool,
}

impl PooledChannel<'_> {
    /// Borrow the underlying [`lapin::Channel`].
    #[must_use]
    pub fn channel(&self) -> &Channel {
        self.channel
            .as_ref()
            .expect("channel is taken only on drop")
    }
}

impl Drop for PooledChannel<'_> {
    fn drop(&mut self) {
        let Some(channel) = self.channel.take() else {
            return;
        };
        if !channel.status().connected() {
            return;
        }
        if let Ok(mut idle) = self.pool.idle.try_lock() {
            if idle.len() < self.pool.max_size {
                idle.push_back(channel);
            }
        }
    }
}

// Unit tests for `ChannelPool` need a `RabbitMqConnection`, which can
// only be built against a live broker. The pool is therefore covered by
// the integration test under `tests/integration.rs` and exercised
// end-to-end alongside [`crate::RabbitMqTransport`].
