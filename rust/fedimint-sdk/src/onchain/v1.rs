//! The v1 wallet module (`wallet`): mappings, subscriptions, and the facade operations.

use std::sync::{Arc, Weak};

use fedimint_client::Client;
use fedimint_client_module::ClientModuleInstance;
use fedimint_client_module::transaction::FeeQuoteRequest;
use fedimint_core::bitcoin;
use fedimint_core::core::OperationId;
use fedimint_core::db::{Database, IDatabaseTransactionOpsCoreTyped};
use fedimint_core::module::Amounts;
use fedimint_core::util::BoxStream;
use fedimint_wallet_client::{
    DepositStateV2, WalletClientModule, WalletOperationMeta, WalletOperationMetaVariant,
    WithdrawState,
};
use futures::StreamExt;

use super::driver::{OnchainReceiveDriver, OnchainSendDriver, until_final};
use super::wire;
use super::{
    OnchainQuoteInner, Plan, Terms, add, balance_of, fee_quote_error, from_upstream, internal, now,
    subscribe_error, unreachable_err,
};
use crate::federation::FederationInner;
use crate::operation::{Backfilled, Driver, kinds, write_details_in};
use crate::sdk::SdkInner;
use crate::{
    Address, Amount, Error, ErrorCode, OnchainReceive, OnchainReceiveDetails,
    OnchainReceiveFeeBreakdown, OnchainReceiveState, OnchainSendDetails, OnchainSendFeeBreakdown,
    OnchainSendState, Operation, Result, Sats, Txid,
};

/// The v1 module on a live client, or `NotSupported` when the federation dropped it.
pub(super) fn module_of(client: &Client) -> Result<ClientModuleInstance<'_, WalletClientModule>> {
    client
        .get_first_module::<WalletClientModule>()
        .map_err(|_| {
            Error::new(
                ErrorCode::NotSupported,
                "this federation no longer has a v1 wallet module",
            )
        })
}

/// Prices a v1 withdrawal: the on-chain miner fee `get_withdraw_fees` reports plus the
/// federation's own fee for building the output ([`OnchainSendFeeBreakdown::wallet_output`]),
/// the cost of funding it from the balance (`funding`), and the dust that funding leaves behind
/// (`change`). Returns the [`Plan`] together with the raw upstream fee, since [`send`] needs the
/// exact same value it just verified the total against, not a second, possibly different, quote.
async fn terms_for(
    client: &Client,
    module: &ClientModuleInstance<'_, WalletClientModule>,
    address: &bitcoin::Address,
    amount: Sats,
) -> Result<(Plan, fedimint_wallet_common::PegOutFees)> {
    let btc_amount = bitcoin::Amount::from_sat(amount.sats());
    let miner_fee = module
        .get_withdraw_fees(address, btc_amount)
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
            .amount()
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
            terms: Terms::V1,
        },
        miner_fee,
    ))
}

pub(super) async fn quote(
    client: &Client,
    module: &ClientModuleInstance<'_, WalletClientModule>,
    address: &bitcoin::Address,
    amount: Sats,
) -> Result<Plan> {
    let (plan, _fee) = terms_for(client, module, address, amount).await?;
    Ok(plan)
}

/// Executes a v1 quote: re-derives the fee quote fresh, refuses on drift, withdraws, records.
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
    let (fresh, fee) = terms_for(client, module, address, quote.amount).await?;
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
        .withdraw(address, btc_amount, fee, wire::custom_meta(&wire_details)?)
        .await
        .map_err(|err| unreachable_err(err.to_string()))?;
    federation
        .create_operation(
            id,
            kinds::ONCHAIN_SEND,
            "wallet",
            &wire_details,
            Arc::new(OnchainSendDriver) as Arc<dyn Driver<OnchainSendState>>,
        )
        .await
}

/// A fresh stream over a v1 withdrawal.
///
/// The client guard is held only for the bounded upstream read that produces the stream; the
/// stream itself registers with the module's notifier when first polled, as the driver contract
/// requires.
pub(super) async fn subscribe_send(
    federation: &FederationInner,
    id: OperationId,
) -> Result<BoxStream<'static, Result<OnchainSendState>>> {
    let client = federation.client(false).await?;
    let module = module_of(&client)?;
    let upstream = module
        .subscribe_withdraw_updates(id)
        .await
        .map_err(|err| subscribe_error(err.to_string()))?
        .into_stream();
    Ok(until_final(Box::pin(
        upstream.map(|state| Ok(map_withdraw(&state))),
    )))
}

// Upstream `WithdrawState` onto `OnchainSendState`. `Failed` upstream means the funding
// transaction was rejected by the federation before anything left the balance, which is why it
// maps to `Refunded` and not `Failed`: the ordinary "rejected, try again" ending, with the money
// safe. There is no state under `v1` that carries an accepted funding with no transaction
// produced, so `OnchainSendState::Failed` is never reached from this module.
fn map_withdraw(state: &WithdrawState) -> OnchainSendState {
    match state {
        WithdrawState::Created => OnchainSendState::Created,
        WithdrawState::Succeeded(txid) => OnchainSendState::Succeeded {
            txid: Txid::from_upstream(*txid),
        },
        WithdrawState::Failed(reason) => OnchainSendState::Refunded {
            reason: reason.clone(),
        },
    }
}

/// A fresh stream over a v1 deposit, filling in the details record as each new fact (the funding
/// transaction, the gross amount, and once claimed, the fee and net credit) is established.
pub(super) async fn subscribe_receive(
    federation: &FederationInner,
    id: OperationId,
) -> Result<BoxStream<'static, Result<OnchainReceiveState>>> {
    let client = federation.client(false).await?;
    let module = module_of(&client)?;
    let upstream = module
        .subscribe_deposit(id)
        .await
        .map_err(|err| subscribe_error(err.to_string()))?
        .into_stream();
    let db = federation.db();
    let sdk = federation.sdk.clone();
    let federation_id = federation.id;
    let stream = upstream.then(move |state| {
        let db = db.clone();
        let sdk = sdk.clone();
        async move { step(&db, &sdk, federation_id, id, state).await }
    });
    Ok(until_final(Box::pin(stream)))
}

/// Maps one upstream deposit state, filling in the parts of the details record it establishes
/// before returning the mapped state so the two never disagree about what was seen.
async fn step(
    db: &Database,
    sdk: &Weak<SdkInner>,
    federation_id: fedimint_core::config::FederationId,
    id: OperationId,
    state: DepositStateV2,
) -> Result<OnchainReceiveState> {
    match state {
        DepositStateV2::WaitingForTransaction => Ok(OnchainReceiveState::WaitingForTransaction),
        DepositStateV2::WaitingForConfirmation {
            btc_deposited,
            btc_out_point,
        } => {
            let txid = Txid::from_upstream(btc_out_point.txid);
            let gross_deposited = Sats::from_sats(btc_deposited.to_sat());
            update_receive_wire(db, id, |w| {
                w.txid = Some(txid.to_string());
                w.gross_deposited_sats = Some(gross_deposited.sats());
            })
            .await?;
            Ok(OnchainReceiveState::WaitingForConfirmation {
                txid,
                gross_deposited,
            })
        }
        DepositStateV2::Confirmed {
            btc_deposited,
            btc_out_point,
        } => {
            let txid = Txid::from_upstream(btc_out_point.txid);
            let gross_deposited = Sats::from_sats(btc_deposited.to_sat());
            update_receive_wire(db, id, |w| {
                w.txid = Some(txid.to_string());
                w.gross_deposited_sats = Some(gross_deposited.sats());
            })
            .await?;
            Ok(OnchainReceiveState::Confirmed {
                txid,
                gross_deposited,
            })
        }
        DepositStateV2::Claimed {
            btc_deposited,
            btc_out_point,
        } => {
            let txid = Txid::from_upstream(btc_out_point.txid);
            let gross_deposited = Sats::from_sats(btc_deposited.to_sat());
            let closed = || {
                Error::new(
                    ErrorCode::FederationClosed,
                    "this federation stopped running",
                )
            };
            let sdk = sdk.upgrade().ok_or_else(closed)?;
            let federation = sdk.federation_inner(&federation_id).ok_or_else(closed)?;
            let client = federation.client(false).await?;
            let breakdown = compute_receive_fee_breakdown(&client, gross_deposited).await?;
            let fee = add(
                add(breakdown.peg_in, breakdown.network_claim)?,
                add(breakdown.primary_module, breakdown.dust)?,
            )?;
            let net_credit = gross_deposited
                .to_amount()
                .and_then(|gross| gross.checked_sub(fee))
                .ok_or_else(|| internal("the deposit fee exceeds the gross amount deposited"))?;
            update_receive_wire(db, id, |w| {
                w.txid = Some(txid.to_string());
                w.gross_deposited_sats = Some(gross_deposited.sats());
                w.fee_msats = Some(fee.msats());
                w.fee_breakdown = Some(wire::OnchainReceiveFeeBreakdownWire::from(&breakdown));
                w.net_credit_msats = Some(net_credit.msats());
            })
            .await?;
            Ok(OnchainReceiveState::Claimed {
                txid,
                gross_deposited,
                net_credit,
            })
        }
        DepositStateV2::Failed(reason) => Ok(OnchainReceiveState::Failed { reason }),
    }
}

/// What claiming a deposit of `gross` costs under v1: the module's own flat peg-in fee
/// ([`fedimint_wallet_common::config::FeeConsensus::peg_in_abs`], read through the generic,
/// module-agnostic dry run [`Client::fee_quote`] runs so that whatever the primary module pulls
/// in to balance the claim is captured too), no separate on-chain network claim step (v1 claims
/// the full gross in one federation transaction and charges only in-transaction fees), the
/// primary module's own cost of minting the credited notes, and the dust that minting leaves
/// behind.
///
/// There is no `receive_fee_quote` on the v1 wallet module the way lightning has one: a deposit
/// address is allocated before any amount is known, so the only point at which this can be priced
/// is once the gross amount has actually arrived, here, at [`DepositStateV2::Claimed`].
async fn compute_receive_fee_breakdown(
    client: &Client,
    gross: Sats,
) -> Result<OnchainReceiveFeeBreakdown> {
    let module = module_of(client)?;
    let peg_in_abs = module.get_fee_consensus().peg_in_abs;
    let gross_amount = gross
        .to_amount()
        .ok_or_else(|| internal("the gross deposit does not fit in millisatoshis"))?;
    let quote = client
        .fee_quote(
            fedimint_core::core::OperationId::new_random(),
            FeeQuoteRequest {
                input_amount: Amounts::new_bitcoin(super::to_upstream(gross_amount)),
                output_amount: Amounts::ZERO,
                input_fee: Amounts::new_bitcoin(peg_in_abs),
                output_fee: Amounts::ZERO,
            },
        )
        .await
        .map_err(|err| internal(err.to_string()))?;
    Ok(OnchainReceiveFeeBreakdown {
        peg_in: from_upstream(quote.input.get_bitcoin()),
        network_claim: Amount::from_msats(0),
        primary_module: from_upstream(quote.output.get_bitcoin()),
        dust: from_upstream(quote.dust.get_bitcoin()),
    })
}

/// Replaces the details JSON of one receive record with `mutate` applied, or does nothing if the
/// record has gone. Read-mutate-write rather than a partial patch, matching
/// `crate::operation::write_details_in`'s own no-op-on-equal behaviour: reapplying the same
/// values on a resubscribe (every v1 deposit subscription replays its sequence from the start,
/// there is no server-side cursor to resume from) writes nothing.
async fn update_receive_wire(
    db: &Database,
    id: OperationId,
    mutate: impl FnOnce(&mut wire::OnchainReceiveDetailsWire),
) -> Result<()> {
    let mut dbtx = db.begin_transaction_nc().await;
    let Some(record) = dbtx.get_value(&crate::db::OperationRecordKey(id)).await else {
        return Ok(());
    };
    drop(dbtx);
    let mut details = wire::decode_receive_wire(&record.details)?;
    mutate(&mut details);
    write_details_in(db, id, wire::encode_receive_wire(&details)?).await
}

/// Allocates a v1 deposit address and records the operation.
///
/// No `extra_meta` copy of the details record is needed the way lightning's receive carries one:
/// the address is already the module's own `WalletOperationMetaVariant::Deposit::address`, so a
/// record rebuilt from the log entry after a crash reads it from there directly; see [`backfill`].
pub(super) async fn receive(
    federation: &Arc<FederationInner>,
    _client: &Client,
    module: &ClientModuleInstance<'_, WalletClientModule>,
) -> Result<OnchainReceive> {
    let created_at = now();
    let info = module
        .safe_allocate_deposit_address(serde_json::Value::Null)
        .await
        .map_err(|err| unreachable_err(err.to_string()))?;
    let address = Address::from_upstream(info.address.as_unchecked().clone());
    let details = OnchainReceiveDetails {
        address: address.clone(),
        txid: None,
        gross_deposited: None,
        fee: None,
        fee_breakdown: None,
        net_credit: None,
        created_at,
    };
    let operation = federation
        .create_operation(
            info.operation_id,
            kinds::ONCHAIN_RECEIVE,
            "wallet",
            &wire::OnchainReceiveDetailsWire::from(&details),
            Arc::new(OnchainReceiveDriver) as Arc<dyn Driver<OnchainReceiveState>>,
        )
        .await?;
    Ok(OnchainReceive { address, operation })
}

/// Rebuilds a record from a v1 log entry: exact for a deposit (the address is upstream's own
/// meta, and nothing else is knowable before a transaction arrives) and for a withdrawal this SDK
/// created, whose custom metadata carries the quoted fee and total verbatim; an estimate — the
/// on-chain miner fee alone, with no federation-side component — for a withdrawal entry the log
/// holds that this SDK did not create.
pub(super) fn backfill(meta: &serde_json::Value, created_at: u64) -> Option<Backfilled> {
    let meta: WalletOperationMeta = serde_json::from_value(meta.clone()).ok()?;
    let created_at = crate::Timestamp::from_epoch_millis(created_at);
    match meta.variant {
        WalletOperationMetaVariant::Deposit { address, .. } => {
            let details = OnchainReceiveDetails {
                address: Address::from_upstream(address),
                txid: None,
                gross_deposited: None,
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
        WalletOperationMetaVariant::Withdraw {
            address,
            amount,
            fee,
            ..
        } => {
            let address = Address::from_upstream(address);
            let amount_sats = Sats::from_sats(amount.to_sat());
            let copy = wire::from_custom_meta::<wire::OnchainSendDetailsWire>(&meta.extra_meta)
                .filter(|copy| copy.address == address.to_string());
            let (fee_amount, total) = match copy {
                Some(copy) => (
                    Amount::from_msats(copy.fee_msats),
                    Amount::from_msats(copy.total_debited_msats),
                ),
                None => {
                    let miner_fee_msats = fee.amount().to_sat().checked_mul(1_000)?;
                    let miner_fee = Amount::from_msats(miner_fee_msats);
                    let total = amount_sats.to_amount()?.checked_add(miner_fee)?;
                    (miner_fee, total)
                }
            };
            let details = OnchainSendDetails {
                address,
                amount: amount_sats,
                fee: fee_amount,
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
        // Deprecated upstream (RBF withdrawals are rejected by the federation) and never an
        // operation this SDK creates.
        WalletOperationMetaVariant::RbfWithdraw { .. } => None,
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
    fn withdraw_states_fold_onto_the_send_lifecycle() {
        assert_eq!(
            map_withdraw(&WithdrawState::Created),
            OnchainSendState::Created
        );
        assert_eq!(
            map_withdraw(&WithdrawState::Succeeded(a_txid())),
            OnchainSendState::Succeeded {
                txid: Txid::from_upstream(a_txid())
            }
        );
        assert_eq!(
            map_withdraw(&WithdrawState::Failed("rejected".to_owned())),
            OnchainSendState::Refunded {
                reason: "rejected".to_owned()
            }
        );
    }

    #[test]
    fn a_deposit_log_entry_backfills_a_waiting_receive_record() {
        let meta = serde_json::json!({
            "variant": {
                "deposit": {
                    "address": an_address().to_string(),
                }
            },
            "extra_meta": null,
        });
        let backfilled = backfill(&meta, 9).expect("claimed");
        assert_eq!(backfilled.kind, kinds::ONCHAIN_RECEIVE);
        let details = wire::decode_receive_details(&backfilled.details).expect("decodes");
        assert_eq!(details.address, an_address());
        assert_eq!(details.txid, None);
        assert_eq!(details.created_at.epoch_millis(), 9);
    }

    #[test]
    fn a_withdraw_log_entry_without_a_copy_reports_only_the_miner_fee() {
        let meta = serde_json::json!({
            "variant": {
                "withdraw": {
                    "address": an_address().to_string(),
                    "amount": 100_000,
                    "fee": { "fee_rate": { "sats_per_kvb": 4_000 }, "total_weight": 1_000 },
                    "change": [],
                }
            },
            "extra_meta": null,
        });
        let backfilled = backfill(&meta, 9).expect("claimed");
        let details = wire::decode_send_details(&backfilled.details).expect("decodes");
        assert_eq!(details.amount, Sats::from_sats(100_000));
        // 1_000 weight at 4_000 sat/kvb: `weight_to_vbytes(1_000) * 4_000 / 1000` = 250 * 4 = 1_000 sat.
        assert_eq!(details.fee, Amount::from_msats(1_000_000));
        assert_eq!(details.total, Amount::from_msats(101_000_000));
    }

    #[test]
    fn a_withdraw_log_entry_with_a_copy_uses_the_copys_fee_and_total() {
        let copy = wire::OnchainSendDetailsWire {
            address: an_address().to_string(),
            amount_sats: 100_000,
            fee_msats: 1_500_000,
            total_debited_msats: 101_500_000,
            fee_breakdown: wire::OnchainSendFeeBreakdownWire {
                wallet_output_msats: 1_000_000,
                funding_msats: 400_000,
                change_msats: 100_000,
            },
            created_at_epoch_ms: 1_650_000_000_000,
        };
        let meta = serde_json::json!({
            "variant": {
                "withdraw": {
                    "address": an_address().to_string(),
                    "amount": 100_000,
                    "fee": { "fee_rate": { "sats_per_kvb": 4_000 }, "total_weight": 1_000 },
                    "change": [],
                }
            },
            "extra_meta": wire::custom_meta(&copy).expect("encode"),
        });
        let backfilled = backfill(&meta, 9).expect("claimed");
        let details = wire::decode_send_details(&backfilled.details).expect("decodes");
        assert_eq!(details.fee, Amount::from_msats(1_500_000));
        assert_eq!(details.total, Amount::from_msats(101_500_000));
    }
}
