//! Per-dispatch terminals plugged into the middleware pipeline.
//!
//! `Next::run` calls `Terminal::dispatch(&self, envelope, ctx)` once the
//! middleware chain is exhausted. Because `dispatch` takes `&self`, the
//! command or query value cannot be moved out of the terminal directly;
//! it is parked in a `Mutex<Option<_>>` and taken on the first call.
//! Re-entry (a middleware that calls `next.run` twice) is detected and
//! surfaced as `HexeractError::Dispatch`.

use std::sync::{Arc, Mutex};

use hexeract_core::{HandlerContext, HexeractError, MessageEnvelope, Terminal};

use crate::erased::{
    BoxAny, BoxFuture, BoxOutput, ErasedCommandHandler, ErasedNotificationHandler,
    ErasedQueryHandler,
};

pub(crate) struct CommandTerminal {
    pub(crate) handler: Arc<dyn ErasedCommandHandler>,
    pub(crate) payload: Mutex<Option<BoxAny>>,
}

impl Terminal for CommandTerminal {
    fn dispatch<'a>(
        &'a self,
        _envelope: &'a MessageEnvelope,
        ctx: &'a HandlerContext,
    ) -> BoxFuture<'a, Result<BoxOutput, HexeractError>> {
        let payload = self.payload.lock().expect("payload mutex poisoned").take();
        Box::pin(async move {
            let Some(payload) = payload else {
                return Err(HexeractError::Dispatch(
                    "command terminal called twice".into(),
                ));
            };
            self.handler.handle(payload, ctx).await
        })
    }
}

pub(crate) struct QueryTerminal {
    pub(crate) handler: Arc<dyn ErasedQueryHandler>,
    pub(crate) payload: Mutex<Option<BoxAny>>,
}

impl Terminal for QueryTerminal {
    fn dispatch<'a>(
        &'a self,
        _envelope: &'a MessageEnvelope,
        ctx: &'a HandlerContext,
    ) -> BoxFuture<'a, Result<BoxOutput, HexeractError>> {
        let payload = self.payload.lock().expect("payload mutex poisoned").take();
        Box::pin(async move {
            let Some(payload) = payload else {
                return Err(HexeractError::Dispatch(
                    "query terminal called twice".into(),
                ));
            };
            self.handler.handle(payload, ctx).await
        })
    }
}

pub(crate) struct NotificationTerminal {
    pub(crate) handler: Arc<dyn ErasedNotificationHandler>,
    pub(crate) payload: Mutex<Option<BoxAny>>,
}

impl Terminal for NotificationTerminal {
    fn dispatch<'a>(
        &'a self,
        _envelope: &'a MessageEnvelope,
        ctx: &'a HandlerContext,
    ) -> BoxFuture<'a, Result<BoxOutput, HexeractError>> {
        let payload = self.payload.lock().expect("payload mutex poisoned").take();
        Box::pin(async move {
            let Some(payload) = payload else {
                return Err(HexeractError::Dispatch(
                    "notification terminal called twice".into(),
                ));
            };
            self.handler.handle(payload, ctx).await?;
            Ok(Box::new(()) as BoxOutput)
        })
    }
}
