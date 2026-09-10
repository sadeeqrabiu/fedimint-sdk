//! The v2 wallet module (`walletv2`): mappings, subscriptions, and the facade operations.
//!
//! `walletv2` is a narrower public surface than `wallet`: `SendSMState` and `ReceiveSMState`
//! live in private modules upstream, reachable only through `await_final_send_operation_state`
//! and `await_receive`, both of which block until the terminal outcome rather than offering a
//! `subscribe_*`-shaped progress stream. And unlike `wallet`'s per-address `safe_allocate_deposit_
//! address` plus `subscribe_deposit`, `walletv2`'s `receive()` hands out the next address from one
//! shared, client-wide pool, and every deposit any of those addresses ever receives resolves
//! through the same client-wide event log and the same `await_receive` call — there is no
//! per-address watch to subscribe to. Both of those shape the mappings below: a send is a
//! two-state stream (`Created`, then the terminal, with no intermediate `wallet`-style
//! `WithdrawState` has none either at the SDK level, so no information is actually lost), and a
//! receive's watch has to read the client-wide event log to tell one address's payment apart from
//! another's.

use std::sync::{Arc, Weak};

use fedimint_client::Client;
use fedimint_client_module::ClientModuleInstance;
use fedimint_client_module::transaction::FeeQuoteRequest;
use fedimint_core::bitcoin;
use fedimint_core::bitcoin::address::NetworkUnchecked;
use fedimint_core::core::OperationId;
use fedimint_core::module::Amounts;
use fedimint_core::util::BoxStream;
use fedimint_eventlog::{Event, EventLogId};
use fedimint_walletv2_client::events::ReceivePaymentEvent;
use fedimint_walletv2_client::{
    FinalReceiveOperationState, FinalSendOperationState, WalletClientModule, WalletOperationMeta,
};
use futures::StreamExt;
use futures::stream;

use super::driver::{OnchainReceiveDriver, OnchainSendDriver, until_final};
use super::wire;
use super::{
    OnchainQuoteInner, Plan, Terms, add, balance_of, fee_quote_error, from_upstream, internal, now,
    subscribe_error, to_upstream, unreachable_err,
};
use crate::federation::FederationInner;
use crate::operation::{Backfilled, Driver, kinds};
use crate::sdk::SdkInner;
use crate::{
    Address, Amount, Error, ErrorCode, OnchainReceive, OnchainReceiveDetails,
    OnchainReceiveFeeBreakdown, OnchainReceiveState, OnchainSendDetails, OnchainSendFeeBreakdown,
    OnchainSendState, Operation, Result, Sats, Timestamp, Txid,
};

/// The v2 module on a live client, or `NotSupported` when the federation dropped it.
pub(super) fn module_of(client: &Client) -> Result<ClientModuleInstance<'_, WalletClientModule>> {
    client
        .get_first_module::<WalletClientModule>()
        .map_err(|_| {
            Error::new(
                ErrorCode::NotSupported,
                "this federation no longer has a v2 wallet module",
            )
        })
}

/// The `walletv2` module's fee consensus, from the client's decoded configuration: the module
/// keeps its own copy private (mirroring `lightning::v2::fee_consensus`, the same problem for the
/// same reason).
async fn fee_consensus(client: &Client) -> Result<fedimint_walletv2_common::config::FeeConsensus> {
    let config = client.config().await;
    let (_, config) = config
        .get_first_module_by_kind::<fedimint_walletv2_common::config::WalletClientConfig>(
            "walletv2",
        )
        .map_err(|err| {
            Error::new(
                ErrorCode::NotSupported,
                format!("this federation's walletv2 configuration is unreadable: {err}"),
            )
        })?;
    Ok(config.fee_consensus.clone())
}

/// Prices a v2 withdrawal: the on-chain miner fee `send_fee` reports plus the federation's own
/// fee for building the output, the cost of funding it from the balance, and the dust that
/// funding leaves behind. Returns the [`Plan`] together with the raw miner fee, since [`send`]
/// needs the exact same value it just verified the total against.
async fn terms_for(
    client: &Client,
    module: &ClientModuleInstance<'_, WalletClientModule>,
    amount: Sats,
) -> Result<(Plan, bitcoin::Amount)> {
    let btc_amount = bitcoin::Amount::from_sat(amount.sats());
    let miner_fee = module
        .send_fee()
        .await
        .map_err(|err| unreachable_err(err.to_string()))?;
    let amount_msats = amount
        .to_amount()
        .ok_or_else(|| internal("the withdrawal amount does not fit in millisatoshis"))?;
    let quote = match module.send_fee_quote(btc_amount).await {
        Ok(quote) => quote,
        Err(err) => return Err(fee_quote_error(client, &err.to_string(), amount_msats).await),
    };
    let module_output = from_upstream(quote.output.get_bitcoin());
    let miner_fee_msats = Amount::from_msats(
        miner_fee
            .to_sat()
            .checked_mul(1_000)
            .ok_or_else(|| internal("the on-chain miner fee overflows millisatoshis"))?,
    );
    let wallet_output = add(module_output, miner_fee_msats)?;
    let funding = from_upstream(quote.input.get_bitcoin());
    let change = from_upstream(quote.dust.get_bitcoin());
    let breakdown = OnchainSendFeeBreakdown {
        wallet_output,
        funding,
        change,
    };
    let fee = add(add(wallet_output, funding)?, change)?;
    let total = add(amount_msats, fee)?;
    Ok((
        Plan {
            breakdown,
            fee,
            total,
            terms: Terms::V2,
        },
        miner_fee,
    ))
}

/// `address` is validated by the facade before this runs; `walletv2`'s own fee quote does not
/// need it (unlike `wallet`'s, its miner-fee estimate is a flat per-transaction figure, not one
/// that varies with the destination script).
pub(super) async fn quote(
    client: &Client,
    module: &ClientModuleInstance<'_, WalletClientModule>,
    _address: &bitcoin::Address,
    amount: Sats,
) -> Result<Plan> {
    let (plan, _fee) = terms_for(client, module, amount).await?;
    Ok(plan)
}

fn send_error(err: fedimint_walletv2_client::SendError) -> Error {
    use fedimint_walletv2_client::SendError;
    match err {
        SendError::WrongNetwork => Error::new(
            ErrorCode::NetworkMismatch,
            "the address is for a different network than the federation",
        ),
        SendError::DustValue => Error::new(
            ErrorCode::InvalidInput,
            "the amount is below this address's dust limit",
        ),
        SendError::UnsupportedAddress => Error::new(
            ErrorCode::InvalidInput,
            "this address type is not supported for an on-chain withdrawal",
        ),
        SendError::InsufficientFunds => Error::new(ErrorCode::InsufficientBalance, err.to_string()),
        SendError::NoConsensusFeerateAvailable | SendError::FederationError(_) => {
            unreachable_err(err)
        }
    }
}

/// Executes a v2 quote: re-derives the fee quote fresh, refuses on drift, sends, records.
pub(super) async fn send(
    federation: &Arc<FederationInner>,
    client: &Client,
    module: &ClientModuleInstance<'_, WalletClientModule>,
    quote: &OnchainQuoteInner,
    address: &bitcoin::Address,
) -> Result<Operation<OnchainSendState>> {
    let available = balance_of(client).await?;
    if available < quote.plan.total {
        return Err(super::insufficient(quote.plan.total, available));
    }
    let (fresh, fee) = terms_for(client, module, quote.amount).await?;
    if fresh.total != quote.plan.total {
        return Err(super::quote_changed(quote.plan.total, fresh.total));
    }
    let created_at = now();
    let details = OnchainSendDetails {
        address: quote.address.clone(),
        amount: quote.amount,
        fee: fresh.fee,
        total: fresh.total,
        created_at,
    };
    let mut wire_details = wire::OnchainSendDetailsWire::from(&details);
    wire_details.fee_breakdown = wire::OnchainSendFeeBreakdownWire::from(&fresh.breakdown);
    let btc_amount = bitcoin::Amount::from_sat(quote.amount.sats());
    let id = module
        .send(
            address.clone().into_unchecked(),
            btc_amount,
            Some(fee),
            wire::custom_meta(&wire_details)?,
        )
        .await
        .map_err(send_error)?;
    federation
        .create_operation(
            id,
            kinds::ONCHAIN_SEND,
            "walletv2",
            &wire_details,
            Arc::new(OnchainSendDriver) as Arc<dyn Driver<OnchainSendState>>,
        )
        .await
}

fn map_final_send(state: FinalSendOperationState) -> OnchainSendState {
    match state {
        FinalSendOperationState::Success(txid) => OnchainSendState::Succeeded {
            txid: Txid::from_upstream(txid),
        },
        FinalSendOperationState::Aborted => OnchainSendState::Refunded {
            reason: "the funding transaction was aborted".to_owned(),
        },
        FinalSendOperationState::Failure => OnchainSendState::Failed {
            reason: "a programming error occurred, or this federation is misbehaving".to_owned(),
        },
    }
}

/// A fresh stream over a v2 withdrawal.
///
/// The client guard is dropped as soon as an owned handle on the module is in hand: unlike
/// `wallet`'s `subscribe_withdraw_updates`, `await_final_send_operation_state` blocks for as long
/// as the withdrawal takes to settle, which must never happen while holding the federation's
/// client guard (see the `Driver::subscribe` contract).
pub(super) async fn subscribe_send(
    federation: &FederationInner,
    id: OperationId,
) -> Result<BoxStream<'static, Result<OnchainSendState>>> {
    let owned = {
        let client = federation.client(false).await?;
        (*module_of(&client)?).clone()
    };
    let stream =
        stream::once(async { Ok(OnchainSendState::Created) }).chain(stream::once(async move {
            owned
                .await_final_send_operation_state(id)
                .await
                .map(map_final_send)
                .map_err(|err| subscribe_error(err.to_string()))
        }));
    Ok(until_final(Box::pin(stream)))
}

/// Allocates the next address from `walletv2`'s shared pool and records the operation.
///
/// The event log position is read before the address is allocated and persisted on the record, so
/// the driver's watch (see [`subscribe_receive`]) starts scanning from no later than the moment a
/// payment to this address could first appear, and resumes from the same point after a restart.
pub(super) async fn receive(
    federation: &Arc<FederationInner>,
    client: &Client,
    module: &ClientModuleInstance<'_, WalletClientModule>,
) -> Result<OnchainReceive> {
    let created_at = now();
    let position = client.get_next_event_log_id().await;
    let checked_address = module.receive().await;
    let address = Address::from_upstream(checked_address.as_unchecked().clone());
    let id = OperationId::new_random();
    let details = OnchainReceiveDetails {
        address: address.clone(),
        txid: None,
        gross_deposited: None,
        fee: None,
        fee_breakdown: None,
        net_credit: None,
        created_at,
    };
    let mut wire_details = wire::OnchainReceiveDetailsWire::from(&details);
    wire_details.event_log_position = Some(position.to_string());
    let operation = federation
        .create_operation(
            id,
            kinds::ONCHAIN_RECEIVE,
            "walletv2",
            &wire_details,
            Arc::new(OnchainReceiveDriver) as Arc<dyn Driver<OnchainReceiveState>>,
        )
        .await?;
    Ok(OnchainReceive { address, operation })
}

/// The final state a resubscribe can already read off the record, without watching again:
/// `walletv2` only ever fills these fields in together, at the one moment its watch resolves (see
/// [`persist_success`] and [`persist_aborted`] in [`watch`]), so their presence is exactly the
/// finality test. Without this, a resubscribe of an operation that already resolved would call
/// `await_receive` again and could block indefinitely: nothing promises another address this
/// client controls will ever be paid.
fn already_resolved(
    details: &wire::OnchainReceiveDetailsWire,
) -> Result<Option<OnchainReceiveState>> {
    let Some(txid) = details.txid.as_deref() else {
        return Ok(None);
    };
    let Some(gross_deposited_sats) = details.gross_deposited_sats else {
        return Ok(None);
    };
    let txid = parse_txid(txid)?;
    let gross_deposited = Sats::from_sats(gross_deposited_sats);
    Ok(Some(match details.net_credit_msats {
        Some(net_credit_msats) => OnchainReceiveState::Claimed {
            txid,
            gross_deposited,
            net_credit: Amount::from_msats(net_credit_msats),
        },
        None => OnchainReceiveState::Failed {
            reason: "the federation rejected the claiming transaction".to_owned(),
        },
    }))
}

/// A fresh stream over a v2 deposit.
///
/// Yields `WaitingForTransaction` immediately, then watches the client-wide event log (see the
/// module doc comment) until a payment to this operation's own address resolves, filling in the
/// details record with what was established the moment it does.
pub(super) async fn subscribe_receive(
    federation: &FederationInner,
    id: OperationId,
    details: wire::OnchainReceiveDetailsWire,
) -> Result<BoxStream<'static, Result<OnchainReceiveState>>> {
    if let Some(state) = already_resolved(&details)? {
        return Ok(Box::pin(stream::iter([Ok(state)])));
    }
    let address = parse_unchecked_address(&details.address)?;
    let position = parse_position(details.event_log_position.as_deref())?;
    let sdk = federation.sdk.clone();
    let federation_id = federation.id;
    let stream = stream::once(async { Ok(OnchainReceiveState::WaitingForTransaction) }).chain(
        stream::once(async move { watch(&sdk, federation_id, id, &address, position).await }),
    );
    Ok(until_final(Box::pin(stream)))
}

/// Loops `await_receive`, skipping every resolution that is not for `address`, until one is,
/// persists what it established, and returns the mapped terminal state.
///
/// A fresh client guard is taken for each bounded step (getting an owned module handle, and later
/// reading the event log) and dropped before the indefinite `await_receive` wait, exactly as
/// [`subscribe_send`] does; see the `Driver::subscribe` contract.
async fn watch(
    sdk: &Weak<SdkInner>,
    federation_id: fedimint_core::config::FederationId,
    id: OperationId,
    address: &bitcoin::Address<NetworkUnchecked>,
    mut position: EventLogId,
) -> Result<OnchainReceiveState> {
    let closed = || {
        Error::new(
            ErrorCode::FederationClosed,
            "this federation stopped running",
        )
    };
    loop {
        let sdk_arc = sdk.upgrade().ok_or_else(closed)?;
        let federation = sdk_arc
            .federation_inner(&federation_id)
            .ok_or_else(closed)?;
        let owned = {
            let client = federation.client(false).await?;
            (*module_of(&client)?).clone()
        };
        let (final_state, next_position) = owned
            .await_receive(position.clone())
            .await
            .map_err(|err| subscribe_error(err.to_string()))?;
        let event = {
            let client = federation.client(false).await?;
            find_receive_event(&client, address, position.clone(), next_position.clone()).await
        };
        position = next_position;
        let Some(event) = event else {
            // Some other address this client controls was paid; keep watching from where this
            // step left off.
            continue;
        };
        let gross_deposited = Sats::from_sats(event.value.to_sat());
        let txid = event.outpoint.map(|out| Txid::from_upstream(out.txid));
        let mapped = match final_state {
            FinalReceiveOperationState::Success => {
                let client = federation.client(false).await?;
                let breakdown =
                    compute_receive_fee_breakdown(&client, gross_deposited, event.fee).await?;
                let fee = add(
                    add(breakdown.peg_in, breakdown.network_claim)?,
                    add(breakdown.primary_module, breakdown.dust)?,
                )?;
                let net_credit = gross_deposited
                    .to_amount()
                    .and_then(|gross| gross.checked_sub(fee))
                    .ok_or_else(|| {
                        internal("the deposit fee exceeds the gross amount deposited")
                    })?;
                let txid =
                    txid.ok_or_else(|| internal("a claimed deposit has no funding outpoint"))?;
                update_receive_wire(&federation.db(), id, |w| {
                    w.txid = Some(txid.to_string());
                    w.gross_deposited_sats = Some(gross_deposited.sats());
                    w.fee_msats = Some(fee.msats());
                    w.fee_breakdown = Some(wire::OnchainReceiveFeeBreakdownWire::from(&breakdown));
                    w.net_credit_msats = Some(net_credit.msats());
                })
                .await?;
                OnchainReceiveState::Claimed {
                    txid,
                    gross_deposited,
                    net_credit,
                }
            }
            FinalReceiveOperationState::Aborted => {
                update_receive_wire(&federation.db(), id, |w| {
                    w.gross_deposited_sats = Some(gross_deposited.sats());
                    if let Some(txid) = &txid {
                        w.txid = Some(txid.to_string());
                    }
                })
                .await?;
                OnchainReceiveState::Failed {
                    reason: "the federation rejected the claiming transaction".to_owned(),
                }
            }
        };
        return Ok(mapped);
    }
}

/// What claiming a deposit of `gross` costs under v2: the on-chain cost of consolidating it
/// (`network_claim`, the only wallet generation that charges this — v1 claims the full gross in
/// one federation transaction with no separate on-chain step), the module's own consensus fee on
/// what is left once that is deducted ([`fedimint_walletv2_common::config::FeeConsensus::fee`],
/// read through the same generic, module-agnostic dry run `wallet::v1`'s equivalent uses, since
/// `walletv2` exposes no `receive_fee_quote` either), the primary module's own cost of minting
/// the credited notes, and the dust that minting leaves behind.
async fn compute_receive_fee_breakdown(
    client: &Client,
    gross: Sats,
    network_claim_fee: bitcoin::Amount,
) -> Result<OnchainReceiveFeeBreakdown> {
    let network_claim = Amount::from_msats(
        network_claim_fee
            .to_sat()
            .checked_mul(1_000)
            .ok_or_else(|| internal("the on-chain claim fee overflows millisatoshis"))?,
    );
    let gross_amount = gross
        .to_amount()
        .ok_or_else(|| internal("the gross deposit does not fit in millisatoshis"))?;
    let net_of_claim = gross_amount
        .checked_sub(network_claim)
        .ok_or_else(|| internal("the on-chain claim fee exceeds the gross amount deposited"))?;
    let consensus = fee_consensus(client).await?;
    let peg_in_abs = consensus.fee(to_upstream(net_of_claim));
    let quote = client
        .fee_quote(
            fedimint_core::core::OperationId::new_random(),
            FeeQuoteRequest {
                input_amount: Amounts::new_bitcoin(to_upstream(net_of_claim)),
                output_amount: Amounts::ZERO,
                input_fee: Amounts::new_bitcoin(peg_in_abs),
                output_fee: Amounts::ZERO,
            },
        )
        .await
        .map_err(|err| internal(err.to_string()))?;
    Ok(OnchainReceiveFeeBreakdown {
        peg_in: from_upstream(quote.input.get_bitcoin()),
        network_claim,
        primary_module: from_upstream(quote.output.get_bitcoin()),
        dust: from_upstream(quote.dust.get_bitcoin()),
    })
}

/// Finds the `ReceivePaymentEvent` for `address` in the client-wide event log window
/// `[from, to)`. `to` is "just past" the entry `await_receive` resolved for (its own doc
/// comment), so the window is small even though it is read as one generous page rather than a
/// precise range, which `Client::get_event_log` does not offer.
async fn find_receive_event(
    client: &Client,
    address: &bitcoin::Address<NetworkUnchecked>,
    from: EventLogId,
    to: EventLogId,
) -> Option<ReceivePaymentEvent> {
    const WINDOW_PAGE: u64 = 256;
    let entries = client.get_event_log(Some(from), WINDOW_PAGE).await;
    for entry in &entries {
        if entry.id() >= to {
            break;
        }
        if entry.module_kind() != Some(&fedimint_walletv2_common::KIND) {
            continue;
        }
        if entry.kind != ReceivePaymentEvent::KIND {
            continue;
        }
        if let Some(event) = entry.to_event::<ReceivePaymentEvent>()
            && &event.address == address
        {
            return Some(event);
        }
    }
    None
}

/// Replaces the details JSON of one receive record with `mutate` applied, or does nothing if the
/// record has gone. See `v1`'s identical helper for why this is read-mutate-write rather than a
/// partial patch.
async fn update_receive_wire(
    db: &fedimint_core::db::Database,
    id: OperationId,
    mutate: impl FnOnce(&mut wire::OnchainReceiveDetailsWire),
) -> Result<()> {
    use fedimint_core::db::IDatabaseTransactionOpsCoreTyped;

    let mut dbtx = db.begin_transaction_nc().await;
    let Some(record) = dbtx.get_value(&crate::db::OperationRecordKey(id)).await else {
        return Ok(());
    };
    drop(dbtx);
    let mut details = wire::decode_receive_wire(&record.details)?;
    mutate(&mut details);
    crate::operation::write_details_in(db, id, wire::encode_receive_wire(&details)?).await
}

fn parse_txid(text: &str) -> Result<Txid> {
    text.parse().map_err(|err: Error| {
        Error::new(
            ErrorCode::Internal,
            format!("a stored transaction id does not parse: {err}"),
        )
    })
}

fn parse_unchecked_address(text: &str) -> Result<bitcoin::Address<NetworkUnchecked>> {
    let address: Address = text.parse().map_err(|err: Error| {
        Error::new(
            ErrorCode::Internal,
            format!("a stored address does not parse: {err}"),
        )
    })?;
    Ok(address.as_unchecked().clone())
}

fn parse_position(stored: Option<&str>) -> Result<EventLogId> {
    match stored {
        Some(text) => text.parse().map_err(|err| {
            Error::new(
                ErrorCode::Internal,
                format!("a stored event log position does not parse: {err}"),
            )
        }),
        // No stored position is a record from a build predating this field: watching from the
        // very start of the log is always safe, only slower.
        None => Ok(EventLogId::LOG_START),
    }
}

/// Rebuilds a record from a v2 log entry: exact for a send this SDK created, whose custom
/// metadata carries the quoted fee and total verbatim; an estimate — the on-chain miner fee
/// alone — for a send entry the log holds that this SDK did not create. A receive entry is
/// rebuilt with only the address, the gross amount and the funding transaction: `ReceiveMeta`
/// carries no outcome, so the fee and net credit are left unset rather than presumed, exactly the
/// same reasoning [`OnchainReceiveDetails`]'s own documentation gives for why `Failed` carries no
/// amount even when one was seen.
pub(super) fn backfill(meta: &serde_json::Value, created_at: u64) -> Option<Backfilled> {
    let meta: WalletOperationMeta = serde_json::from_value(meta.clone()).ok()?;
    let created_at = Timestamp::from_epoch_millis(created_at);
    match meta {
        WalletOperationMeta::Send(send) => {
            let address = Address::from_upstream(send.address);
            let amount = Sats::from_sats(send.value.to_sat());
            let copy = wire::from_custom_meta::<wire::OnchainSendDetailsWire>(&send.custom_meta)
                .filter(|copy| copy.address == address.to_string());
            let (fee, total) = match copy {
                Some(copy) => (
                    Amount::from_msats(copy.fee_msats),
                    Amount::from_msats(copy.total_debited_msats),
                ),
                None => {
                    let miner_fee_msats = send.fee.to_sat().checked_mul(1_000)?;
                    let miner_fee = Amount::from_msats(miner_fee_msats);
                    let total = amount.to_amount()?.checked_add(miner_fee)?;
                    (miner_fee, total)
                }
            };
            let details = OnchainSendDetails {
                address,
                amount,
                fee,
                total,
                created_at,
            };
            Some(Backfilled {
                kind: kinds::ONCHAIN_SEND,
                details: serde_json::to_string(&wire::OnchainSendDetailsWire::from(&details))
                    .ok()?,
                phase: None,
            })
        }
        WalletOperationMeta::Receive(receive) => {
            let address = Address::from_upstream(receive.address?);
            let gross_deposited = Sats::from_sats(receive.value.to_sat());
            let txid = receive.outpoint.map(|out| Txid::from_upstream(out.txid));
            let details = OnchainReceiveDetails {
                address,
                txid,
                gross_deposited: Some(gross_deposited),
                fee: None,
                fee_breakdown: None,
                net_credit: None,
                created_at,
            };
            Some(Backfilled {
                kind: kinds::ONCHAIN_RECEIVE,
                details: serde_json::to_string(&wire::OnchainReceiveDetailsWire::from(&details))
                    .ok()?,
                phase: None,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn an_address() -> Address {
        "bcrt1q2nfxmhd4n3c8834pj72xagvyr9gl57n5r94fsl"
            .parse()
            .expect("a valid regtest address")
    }

    fn a_txid() -> bitcoin::Txid {
        "0000000000000000000000000000000000000000000000000000000000000000"
            .parse()
            .expect("a well-formed transaction id")
    }

    #[test]
    fn final_send_states_fold_onto_the_send_lifecycle() {
        assert_eq!(
            map_final_send(FinalSendOperationState::Success(a_txid())),
            OnchainSendState::Succeeded {
                txid: Txid::from_upstream(a_txid())
            }
        );
        assert!(matches!(
            map_final_send(FinalSendOperationState::Aborted),
            OnchainSendState::Refunded { .. }
        ));
        assert!(matches!(
            map_final_send(FinalSendOperationState::Failure),
            OnchainSendState::Failed { .. }
        ));
    }

    #[test]
    fn already_resolved_reads_a_claimed_record_without_watching() {
        let details = wire::OnchainReceiveDetailsWire {
            address: an_address().to_string(),
            txid: Some(a_txid().to_string()),
            gross_deposited_sats: Some(100_000),
            fee_msats: Some(1_500),
            fee_breakdown: None,
            net_credit_msats: Some(99_998_500),
            created_at_epoch_ms: 0,
            event_log_position: None,
        };
        let state = already_resolved(&details)
            .expect("decodes")
            .expect("resolved");
        assert_eq!(
            state,
            OnchainReceiveState::Claimed {
                txid: Txid::from_upstream(a_txid()),
                gross_deposited: Sats::from_sats(100_000),
                net_credit: Amount::from_msats(99_998_500),
            }
        );
    }

    #[test]
    fn already_resolved_reads_a_failed_record_without_watching() {
        let details = wire::OnchainReceiveDetailsWire {
            address: an_address().to_string(),
            txid: Some(a_txid().to_string()),
            gross_deposited_sats: Some(100_000),
            fee_msats: None,
            fee_breakdown: None,
            net_credit_msats: None,
            created_at_epoch_ms: 0,
            event_log_position: None,
        };
        let state = already_resolved(&details)
            .expect("decodes")
            .expect("resolved");
        assert!(matches!(state, OnchainReceiveState::Failed { .. }));
    }

    #[test]
    fn already_resolved_is_none_before_a_transaction_is_seen() {
        let details = wire::OnchainReceiveDetailsWire {
            address: an_address().to_string(),
            txid: None,
            gross_deposited_sats: None,
            fee_msats: None,
            fee_breakdown: None,
            net_credit_msats: None,
            created_at_epoch_ms: 0,
            event_log_position: Some("0".to_owned()),
        };
        assert_eq!(already_resolved(&details).expect("decodes"), None);
    }

    #[test]
    fn parse_position_defaults_to_the_log_start_when_absent() {
        assert_eq!(
            parse_position(None).expect("decodes"),
            EventLogId::LOG_START
        );
    }

    #[test]
    fn a_send_log_entry_without_a_copy_reports_only_the_miner_fee() {
        let meta = serde_json::json!({
            "Send": {
                "change_outpoint_range": {
                    "txid": "00".repeat(32),
                    "idx_range": { "start": 0, "end": 1 },
                },
                "address": an_address().to_string(),
                "value": 100_000,
                "fee": 1_000,
                "custom_meta": null,
            }
        });
        let backfilled = backfill(&meta, 9).expect("claimed");
        let details = wire::decode_send_details(&backfilled.details).expect("decodes");
        assert_eq!(details.amount, Sats::from_sats(100_000));
        assert_eq!(details.fee, Amount::from_msats(1_000_000));
        assert_eq!(details.total, Amount::from_msats(101_000_000));
    }

    #[test]
    fn a_receive_log_entry_carries_no_fee_or_net_credit() {
        let meta = serde_json::json!({
            "Receive": {
                "change_outpoint_range": {
                    "txid": "00".repeat(32),
                    "idx_range": { "start": 0, "end": 1 },
                },
                "value": 100_000,
                "fee": 500,
                "address": an_address().to_string(),
                "outpoint": format!("{}:0", a_txid()),
            }
        });
        let backfilled = backfill(&meta, 9).expect("claimed");
        let details = wire::decode_receive_details(&backfilled.details).expect("decodes");
        assert_eq!(details.address, an_address());
        assert_eq!(details.gross_deposited, Some(Sats::from_sats(100_000)));
        assert_eq!(details.fee, None);
        assert_eq!(details.net_credit, None);
    }
}
