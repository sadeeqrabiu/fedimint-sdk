//! The two on-chain drivers, the backfiller, and the stream settling utilities they share.
//!
//! `until_final`/`settled`/`first_state` are the same pattern `lightning::driver` uses, for the
//! same reason: neither wallet module offers a "current state" read, so a point-in-time answer is
//! produced by draining a replayed stream until it settles. Kept as this facade's own copy rather
//! than shared, exactly as the lightning facade's copy is its own.

use core::time::Duration;
use std::any::Any;

use fedimint_core::core::OperationId;
use fedimint_core::task::MaybeSend;
use fedimint_core::util::{BoxFuture, BoxStream};
use futures::{Stream, StreamExt, future};

use super::{v1, v2, wire};
use crate::db::OperationRecord;
use crate::federation::FederationInner;
use crate::operation::{Backfilled, Backfiller, Driver};
use crate::{Error, ErrorCode, OnchainReceiveState, OnchainSendState, OperationState, Result};

/// How long `current` waits for the next replayed state before calling the last one current.
const CURRENT_STATE_SETTLE: Duration = Duration::from_millis(500);

/// Ends a stream after its first final state, and on the first error.
pub(super) fn until_final<S>(
    stream: impl Stream<Item = Result<S>> + MaybeSend + 'static,
) -> BoxStream<'static, Result<S>>
where
    S: OperationState,
{
    Box::pin(stream.scan(false, |done, item| {
        if *done {
            return future::ready(None);
        }
        *done = match &item {
            Ok(state) => state.is_final(),
            Err(_) => true,
        };
        future::ready(Some(item))
    }))
}

/// A subscription that yields the current state first: the replayed history is drained until it
/// settles, the last state it produced is yielded, and every state after that is forwarded as it
/// comes. See `lightning::driver::settled`, which this mirrors exactly.
pub(super) fn settled<S>(stream: BoxStream<'static, Result<S>>) -> BoxStream<'static, Result<S>>
where
    S: OperationState,
{
    Box::pin(futures::stream::unfold(
        (stream, false),
        |(mut stream, started)| async move {
            if started {
                return stream.next().await.map(|item| (item, (stream, true)));
            }
            let mut last = stream.next().await?;
            while matches!(&last, Ok(state) if !state.is_final()) {
                match fedimint_core::runtime::timeout(CURRENT_STATE_SETTLE, stream.next()).await {
                    Ok(Some(item)) => last = item,
                    Ok(None) | Err(_) => break,
                }
            }
            Some((last, (stream, true)))
        },
    ))
}

/// The first state a stream yields, mapping an ended stream to this operation's "no state" error.
pub(super) async fn first_state<S>(mut stream: BoxStream<'static, Result<S>>) -> Result<S>
where
    S: OperationState,
{
    match stream.next().await {
        Some(item) => item,
        None => Err(Error::new(
            ErrorCode::Internal,
            "this operation's subscription yielded no state",
        )),
    }
}

/// Observes an on-chain withdrawal of either generation, chosen by the record's module.
pub(crate) struct OnchainSendDriver;

impl Driver<OnchainSendState> for OnchainSendDriver {
    fn current<'a>(
        &'a self,
        federation: &'a FederationInner,
        id: OperationId,
        record: &'a OperationRecord,
    ) -> BoxFuture<'a, Result<OnchainSendState>> {
        Box::pin(async move {
            if let Some(encoded) = &record.final_state {
                return wire::decode_send_state(encoded);
            }
            first_state(self.subscribe(federation, id, record).await?).await
        })
    }

    fn subscribe<'a>(
        &'a self,
        federation: &'a FederationInner,
        id: OperationId,
        record: &'a OperationRecord,
    ) -> BoxFuture<'a, Result<BoxStream<'static, Result<OnchainSendState>>>> {
        Box::pin(async move {
            let stream = match record.module.as_str() {
                "wallet" => v1::subscribe_send(federation, id).await,
                "walletv2" => v2::subscribe_send(federation, id).await,
                other => Err(unknown_module(other)),
            }?;
            Ok(settled(stream))
        })
    }

    fn same_state(&self, previous: &OnchainSendState, next: &OnchainSendState) -> bool {
        previous == next
    }

    fn encode_state(&self, state: &OnchainSendState) -> Result<String> {
        wire::encode_send_state(state)
    }

    fn decode_details(&self, json: &str) -> Result<Box<dyn Any + Send + Sync>> {
        Ok(Box::new(wire::decode_send_details(json)?))
    }
}

/// Observes an on-chain deposit of either generation.
pub(crate) struct OnchainReceiveDriver;

impl Driver<OnchainReceiveState> for OnchainReceiveDriver {
    fn current<'a>(
        &'a self,
        federation: &'a FederationInner,
        id: OperationId,
        record: &'a OperationRecord,
    ) -> BoxFuture<'a, Result<OnchainReceiveState>> {
        Box::pin(async move {
            if let Some(encoded) = &record.final_state {
                return wire::decode_receive_state(encoded);
            }
            first_state(self.subscribe(federation, id, record).await?).await
        })
    }

    fn subscribe<'a>(
        &'a self,
        federation: &'a FederationInner,
        id: OperationId,
        record: &'a OperationRecord,
    ) -> BoxFuture<'a, Result<BoxStream<'static, Result<OnchainReceiveState>>>> {
        Box::pin(async move {
            let stream = match record.module.as_str() {
                "wallet" => v1::subscribe_receive(federation, id).await,
                "walletv2" => {
                    let details = wire::decode_receive_wire(&record.details)?;
                    v2::subscribe_receive(federation, id, details).await
                }
                other => Err(unknown_module(other)),
            }?;
            Ok(settled(stream))
        })
    }

    fn same_state(&self, previous: &OnchainReceiveState, next: &OnchainReceiveState) -> bool {
        previous == next
    }

    fn encode_state(&self, state: &OnchainReceiveState) -> Result<String> {
        wire::encode_receive_state(state)
    }

    fn decode_details(&self, json: &str) -> Result<Box<dyn Any + Send + Sync>> {
        Ok(Box::new(wire::decode_receive_details(json)?))
    }
}

/// Rebuilds an on-chain record from the module's own log entry, for either generation.
pub(crate) struct OnchainBackfiller;

impl Backfiller for OnchainBackfiller {
    fn backfill(
        &self,
        module_kind: &str,
        meta: &serde_json::Value,
        created_at: u64,
    ) -> Option<Backfilled> {
        match module_kind {
            "wallet" => v1::backfill(meta, created_at),
            "walletv2" => v2::backfill(meta, created_at),
            _ => None,
        }
    }
}

fn unknown_module(module: &str) -> Error {
    Error::new(
        ErrorCode::Internal,
        format!("an on-chain record names a module this build cannot observe: {module:?}"),
    )
}

#[cfg(test)]
mod tests {
    use futures::stream;

    use super::*;

    // Only `OnchainSendState`'s non-final (`Created`) and final (`Refunded`, `Failed`) variants
    // that need no fields are used below; `settled` treats every state the same way regardless of
    // which state enum it is instantiated with.

    #[tokio::test(flavor = "multi_thread")]
    async fn late_subscriber_sees_settled_state_first() {
        let stream: BoxStream<'static, Result<OnchainSendState>> =
            Box::pin(stream::iter([Ok(OnchainSendState::Created)]).chain(stream::pending()));
        let mut settled_stream = settled(stream);
        let first = settled_stream.next().await;
        assert_eq!(
            first.expect("stream ended").expect("stream errored"),
            OnchainSendState::Created
        );
        let second = tokio::time::timeout(Duration::from_millis(50), settled_stream.next()).await;
        assert!(
            second.is_err(),
            "a second item arrived when none should have"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn replay_ending_in_a_final_state_yields_only_that_state() {
        let stream: BoxStream<'static, Result<OnchainSendState>> = Box::pin(stream::iter([
            Ok(OnchainSendState::Created),
            Ok(OnchainSendState::Refunded {
                reason: "rejected".to_owned(),
            }),
        ]));
        let mut settled_stream = settled(stream);
        let first = settled_stream.next().await;
        assert_eq!(
            first.expect("stream ended").expect("stream errored"),
            OnchainSendState::Refunded {
                reason: "rejected".to_owned()
            }
        );
        assert!(settled_stream.next().await.is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn empty_stream_yields_nothing() {
        let stream: BoxStream<'static, Result<OnchainSendState>> = Box::pin(stream::empty());
        let mut settled_stream = settled(stream);
        assert!(settled_stream.next().await.is_none());

        let stream: BoxStream<'static, Result<OnchainSendState>> = Box::pin(stream::empty());
        let err = first_state(stream)
            .await
            .expect_err("an empty stream must not settle");
        assert_eq!(err.code, ErrorCode::Internal);
    }

    #[test]
    fn until_final_stops_after_the_first_final_state() {
        // `until_final` is exercised end to end by `v1`'s and `v2`'s own `subscribe_*` tests via
        // `settled`; this checks the truncation itself in isolation.
        let mapped = until_final(stream::iter([
            Ok(OnchainSendState::Created),
            Ok(OnchainSendState::Failed {
                reason: "boom".to_owned(),
            }),
            Ok(OnchainSendState::Created),
        ]));
        let collected: Vec<_> = futures::executor::block_on(mapped.collect());
        assert_eq!(collected.len(), 2, "{collected:?}");
    }
}
