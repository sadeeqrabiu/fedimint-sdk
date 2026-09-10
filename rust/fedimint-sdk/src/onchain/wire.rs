//! The persisted shapes of the on-chain facade: details records and final states as JSON, and
//! the fee breakdown wires each carries.
//!
//! These are storage format. A field added later must be `Option` with `#[serde(default)]`, and
//! a field is never renamed or removed: every record already written reads through this file.

use serde::{Deserialize, Serialize};

use crate::{
    Address, Amount, Error, ErrorCode, OnchainReceiveDetails, OnchainReceiveFeeBreakdown,
    OnchainReceiveState, OnchainSendDetails, OnchainSendFeeBreakdown, OnchainSendState, Result,
    Sats, Timestamp, Txid,
};

/// The key under which the SDK's own details record rides inside the module's custom metadata,
/// so a record rebuilt from the module's log entry after a crash carries the exact quoted (or
/// allocated) terms rather than an upstream-derived estimate. Shared with the lightning facade's
/// own metadata key: the two never appear on the same module's log entry, so there is no
/// collision to guard against.
pub(super) const CUSTOM_META_KEY: &str = "fedimint_sdk";

/// The details record wrapped for the module's custom metadata.
pub(super) fn custom_meta<W>(wire: &W) -> Result<serde_json::Value>
where
    W: Serialize,
{
    let wire = serde_json::to_value(wire).map_err(encode_error)?;
    Ok(serde_json::json!({ CUSTOM_META_KEY: wire }))
}

/// The details record carried inside the module's custom metadata, if this SDK put one there.
pub(super) fn from_custom_meta<W>(meta: &serde_json::Value) -> Option<W>
where
    W: serde::de::DeserializeOwned,
{
    let wire = meta.as_object()?.get(CUSTOM_META_KEY)?;
    serde_json::from_value(wire.clone()).ok()
}

/// [`OnchainSendFeeBreakdown`] as stored.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct OnchainSendFeeBreakdownWire {
    pub(super) wallet_output_msats: u64,
    pub(super) funding_msats: u64,
    pub(super) change_msats: u64,
}

impl From<&OnchainSendFeeBreakdown> for OnchainSendFeeBreakdownWire {
    fn from(breakdown: &OnchainSendFeeBreakdown) -> OnchainSendFeeBreakdownWire {
        OnchainSendFeeBreakdownWire {
            wallet_output_msats: breakdown.wallet_output.msats(),
            funding_msats: breakdown.funding.msats(),
            change_msats: breakdown.change.msats(),
        }
    }
}

impl From<OnchainSendFeeBreakdownWire> for OnchainSendFeeBreakdown {
    fn from(wire: OnchainSendFeeBreakdownWire) -> OnchainSendFeeBreakdown {
        OnchainSendFeeBreakdown {
            wallet_output: Amount::from_msats(wire.wallet_output_msats),
            funding: Amount::from_msats(wire.funding_msats),
            change: Amount::from_msats(wire.change_msats),
        }
    }
}

/// [`OnchainReceiveFeeBreakdown`] as stored.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct OnchainReceiveFeeBreakdownWire {
    pub(super) peg_in_msats: u64,
    pub(super) network_claim_msats: u64,
    pub(super) primary_module_msats: u64,
    pub(super) dust_msats: u64,
}

impl From<&OnchainReceiveFeeBreakdown> for OnchainReceiveFeeBreakdownWire {
    fn from(breakdown: &OnchainReceiveFeeBreakdown) -> OnchainReceiveFeeBreakdownWire {
        OnchainReceiveFeeBreakdownWire {
            peg_in_msats: breakdown.peg_in.msats(),
            network_claim_msats: breakdown.network_claim.msats(),
            primary_module_msats: breakdown.primary_module.msats(),
            dust_msats: breakdown.dust.msats(),
        }
    }
}

impl From<OnchainReceiveFeeBreakdownWire> for OnchainReceiveFeeBreakdown {
    fn from(wire: OnchainReceiveFeeBreakdownWire) -> OnchainReceiveFeeBreakdown {
        OnchainReceiveFeeBreakdown {
            peg_in: Amount::from_msats(wire.peg_in_msats),
            network_claim: Amount::from_msats(wire.network_claim_msats),
            primary_module: Amount::from_msats(wire.primary_module_msats),
            dust: Amount::from_msats(wire.dust_msats),
        }
    }
}

/// [`OnchainSendDetails`] as stored.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct OnchainSendDetailsWire {
    pub(super) address: String,
    pub(super) amount_sats: u64,
    pub(super) fee_msats: u64,
    pub(super) total_debited_msats: u64,
    pub(super) fee_breakdown: OnchainSendFeeBreakdownWire,
    pub(super) created_at_epoch_ms: u64,
}

impl From<&OnchainSendDetails> for OnchainSendDetailsWire {
    fn from(details: &OnchainSendDetails) -> OnchainSendDetailsWire {
        OnchainSendDetailsWire {
            address: details.address.to_string(),
            amount_sats: details.amount.sats(),
            fee_msats: details.fee.msats(),
            total_debited_msats: details.total.msats(),
            // The breakdown is not carried on `OnchainSendDetails` itself; callers that need it
            // read `OnchainQuote::fee_breakdown` before executing. What is persisted here is
            // reconstructed from the executed quote by whichever module wrote the record; see
            // `v1::send` and `v2::send`.
            fee_breakdown: OnchainSendFeeBreakdownWire {
                wallet_output_msats: 0,
                funding_msats: 0,
                change_msats: 0,
            },
            created_at_epoch_ms: details.created_at.epoch_millis(),
        }
    }
}

impl TryFrom<OnchainSendDetailsWire> for OnchainSendDetails {
    type Error = Error;

    fn try_from(wire: OnchainSendDetailsWire) -> Result<OnchainSendDetails> {
        Ok(OnchainSendDetails {
            address: parse_address(&wire.address)?,
            amount: Sats::from_sats(wire.amount_sats),
            fee: Amount::from_msats(wire.fee_msats),
            total: Amount::from_msats(wire.total_debited_msats),
            created_at: Timestamp::from_epoch_millis(wire.created_at_epoch_ms),
        })
    }
}

/// [`OnchainReceiveDetails`] as stored.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct OnchainReceiveDetailsWire {
    pub(super) address: String,
    pub(super) txid: Option<String>,
    pub(super) gross_deposited_sats: Option<u64>,
    pub(super) fee_msats: Option<u64>,
    pub(super) fee_breakdown: Option<OnchainReceiveFeeBreakdownWire>,
    pub(super) net_credit_msats: Option<u64>,
    pub(super) created_at_epoch_ms: u64,
    /// `walletv2` only: the client-wide event log position this operation's watch resumes from.
    /// `walletv2` has no per-address deposit stream, only a client-wide event log every address's
    /// payment resolves through (see `src/onchain/v2.rs`), so this is what lets a resubscribe
    /// after a restart pick the watch back up without replaying the whole log from the start. Not
    /// part of the public record.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) event_log_position: Option<String>,
}

impl From<&OnchainReceiveDetails> for OnchainReceiveDetailsWire {
    fn from(details: &OnchainReceiveDetails) -> OnchainReceiveDetailsWire {
        OnchainReceiveDetailsWire {
            address: details.address.to_string(),
            txid: details.txid.as_ref().map(ToString::to_string),
            gross_deposited_sats: details.gross_deposited.map(Sats::sats),
            fee_msats: details.fee.map(Amount::msats),
            fee_breakdown: details
                .fee_breakdown
                .as_ref()
                .map(OnchainReceiveFeeBreakdownWire::from),
            net_credit_msats: details.net_credit.map(Amount::msats),
            created_at_epoch_ms: details.created_at.epoch_millis(),
            event_log_position: None,
        }
    }
}

impl TryFrom<OnchainReceiveDetailsWire> for OnchainReceiveDetails {
    type Error = Error;

    fn try_from(wire: OnchainReceiveDetailsWire) -> Result<OnchainReceiveDetails> {
        Ok(OnchainReceiveDetails {
            address: parse_address(&wire.address)?,
            txid: wire.txid.as_deref().map(parse_txid).transpose()?,
            gross_deposited: wire.gross_deposited_sats.map(Sats::from_sats),
            fee: wire.fee_msats.map(Amount::from_msats),
            fee_breakdown: wire.fee_breakdown.map(OnchainReceiveFeeBreakdown::from),
            net_credit: wire.net_credit_msats.map(Amount::from_msats),
            created_at: Timestamp::from_epoch_millis(wire.created_at_epoch_ms),
        })
    }
}

/// [`OnchainSendState`] as stored on `OperationRecord::final_state`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
enum OnchainSendStateWire {
    Created,
    Succeeded { txid: String },
    Refunded { reason: String },
    Failed { reason: String },
}

/// [`OnchainReceiveState`] as stored on `OperationRecord::final_state`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
enum OnchainReceiveStateWire {
    WaitingForTransaction,
    WaitingForConfirmation {
        txid: String,
        gross_deposited_sats: u64,
    },
    Confirmed {
        txid: String,
        gross_deposited_sats: u64,
    },
    Claimed {
        txid: String,
        gross_deposited_sats: u64,
        net_credit_msats: u64,
    },
    Failed {
        reason: String,
    },
}

pub(super) fn encode_send_state(state: &OnchainSendState) -> Result<String> {
    let wire = match state {
        OnchainSendState::Created => OnchainSendStateWire::Created,
        OnchainSendState::Succeeded { txid } => OnchainSendStateWire::Succeeded {
            txid: txid.to_string(),
        },
        OnchainSendState::Refunded { reason } => OnchainSendStateWire::Refunded {
            reason: reason.clone(),
        },
        OnchainSendState::Failed { reason } => OnchainSendStateWire::Failed {
            reason: reason.clone(),
        },
    };
    serde_json::to_string(&wire).map_err(encode_error)
}

pub(super) fn decode_send_state(encoded: &str) -> Result<OnchainSendState> {
    let wire: OnchainSendStateWire = serde_json::from_str(encoded).map_err(decode_error)?;
    Ok(match wire {
        OnchainSendStateWire::Created => OnchainSendState::Created,
        OnchainSendStateWire::Succeeded { txid } => OnchainSendState::Succeeded {
            txid: parse_txid(&txid)?,
        },
        OnchainSendStateWire::Refunded { reason } => OnchainSendState::Refunded { reason },
        OnchainSendStateWire::Failed { reason } => OnchainSendState::Failed { reason },
    })
}

pub(super) fn encode_receive_state(state: &OnchainReceiveState) -> Result<String> {
    let wire = match state {
        OnchainReceiveState::WaitingForTransaction => {
            OnchainReceiveStateWire::WaitingForTransaction
        }
        OnchainReceiveState::WaitingForConfirmation {
            txid,
            gross_deposited,
        } => OnchainReceiveStateWire::WaitingForConfirmation {
            txid: txid.to_string(),
            gross_deposited_sats: gross_deposited.sats(),
        },
        OnchainReceiveState::Confirmed {
            txid,
            gross_deposited,
        } => OnchainReceiveStateWire::Confirmed {
            txid: txid.to_string(),
            gross_deposited_sats: gross_deposited.sats(),
        },
        OnchainReceiveState::Claimed {
            txid,
            gross_deposited,
            net_credit,
        } => OnchainReceiveStateWire::Claimed {
            txid: txid.to_string(),
            gross_deposited_sats: gross_deposited.sats(),
            net_credit_msats: net_credit.msats(),
        },
        OnchainReceiveState::Failed { reason } => OnchainReceiveStateWire::Failed {
            reason: reason.clone(),
        },
    };
    serde_json::to_string(&wire).map_err(encode_error)
}

pub(super) fn decode_receive_state(encoded: &str) -> Result<OnchainReceiveState> {
    let wire: OnchainReceiveStateWire = serde_json::from_str(encoded).map_err(decode_error)?;
    Ok(match wire {
        OnchainReceiveStateWire::WaitingForTransaction => {
            OnchainReceiveState::WaitingForTransaction
        }
        OnchainReceiveStateWire::WaitingForConfirmation {
            txid,
            gross_deposited_sats,
        } => OnchainReceiveState::WaitingForConfirmation {
            txid: parse_txid(&txid)?,
            gross_deposited: Sats::from_sats(gross_deposited_sats),
        },
        OnchainReceiveStateWire::Confirmed {
            txid,
            gross_deposited_sats,
        } => OnchainReceiveState::Confirmed {
            txid: parse_txid(&txid)?,
            gross_deposited: Sats::from_sats(gross_deposited_sats),
        },
        OnchainReceiveStateWire::Claimed {
            txid,
            gross_deposited_sats,
            net_credit_msats,
        } => OnchainReceiveState::Claimed {
            txid: parse_txid(&txid)?,
            gross_deposited: Sats::from_sats(gross_deposited_sats),
            net_credit: Amount::from_msats(net_credit_msats),
        },
        OnchainReceiveStateWire::Failed { reason } => OnchainReceiveState::Failed { reason },
    })
}

pub(super) fn decode_send_details(json: &str) -> Result<OnchainSendDetails> {
    let wire: OnchainSendDetailsWire = serde_json::from_str(json).map_err(decode_error)?;
    OnchainSendDetails::try_from(wire)
}

pub(super) fn decode_receive_details(json: &str) -> Result<OnchainReceiveDetails> {
    OnchainReceiveDetails::try_from(decode_receive_wire(json)?)
}

pub(super) fn decode_receive_wire(json: &str) -> Result<OnchainReceiveDetailsWire> {
    serde_json::from_str(json).map_err(decode_error)
}

pub(super) fn encode_receive_wire(wire: &OnchainReceiveDetailsWire) -> Result<String> {
    serde_json::to_string(wire).map_err(encode_error)
}

pub(super) fn encode_send_details(wire: &OnchainSendDetailsWire) -> Result<String> {
    serde_json::to_string(wire).map_err(encode_error)
}

fn parse_address(text: &str) -> Result<Address> {
    text.parse().map_err(|err: Error| {
        Error::new(
            ErrorCode::Internal,
            format!("a stored address does not parse: {err}"),
        )
    })
}

fn parse_txid(text: &str) -> Result<Txid> {
    text.parse().map_err(|err: Error| {
        Error::new(
            ErrorCode::Internal,
            format!("a stored transaction id does not parse: {err}"),
        )
    })
}

fn encode_error(err: serde_json::Error) -> Error {
    Error::new(
        ErrorCode::Internal,
        format!("could not encode an on-chain record: {err}"),
    )
}

fn decode_error(err: serde_json::Error) -> Error {
    Error::new(
        ErrorCode::Internal,
        format!("could not decode an on-chain record: {err}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn an_address() -> Address {
        "bcrt1q2nfxmhd4n3c8834pj72xagvyr9gl57n5r94fsl"
            .parse()
            .expect("a valid regtest address")
    }

    fn a_txid() -> Txid {
        "0000000000000000000000000000000000000000000000000000000000000000"
            .parse()
            .expect("a well-formed transaction id")
    }

    fn send_details() -> OnchainSendDetails {
        OnchainSendDetails {
            address: an_address(),
            amount: Sats::from_sats(25_000),
            fee: Amount::from_msats(1_234_567),
            total: Amount::from_msats(26_234_567),
            created_at: Timestamp::from_epoch_millis(1_700_000_000_000),
        }
    }

    fn receive_details() -> OnchainReceiveDetails {
        OnchainReceiveDetails {
            address: an_address(),
            txid: Some(a_txid()),
            gross_deposited: Some(Sats::from_sats(100_000)),
            fee: Some(Amount::from_msats(1_500)),
            fee_breakdown: Some(OnchainReceiveFeeBreakdown {
                peg_in: Amount::from_msats(1_000),
                network_claim: Amount::from_msats(0),
                primary_module: Amount::from_msats(400),
                dust: Amount::from_msats(100),
            }),
            net_credit: Some(Amount::from_msats(99_998_500)),
            created_at: Timestamp::from_epoch_millis(1_700_000_000_000),
        }
    }

    #[test]
    fn send_details_round_trip_through_json() {
        let details = send_details();
        let json = serde_json::to_string(&OnchainSendDetailsWire::from(&details)).expect("encode");
        assert_eq!(decode_send_details(&json).expect("decode"), details);
    }

    #[test]
    fn receive_details_round_trip_through_json() {
        let details = receive_details();
        let json =
            serde_json::to_string(&OnchainReceiveDetailsWire::from(&details)).expect("encode");
        assert_eq!(decode_receive_details(&json).expect("decode"), details);
    }

    #[test]
    fn a_receive_before_any_transaction_round_trips_with_every_field_absent() {
        let details = OnchainReceiveDetails {
            address: an_address(),
            txid: None,
            gross_deposited: None,
            fee: None,
            fee_breakdown: None,
            net_credit: None,
            created_at: Timestamp::from_epoch_millis(0),
        };
        let json =
            serde_json::to_string(&OnchainReceiveDetailsWire::from(&details)).expect("encode");
        assert_eq!(decode_receive_details(&json).expect("decode"), details);
    }

    #[test]
    fn a_failed_receive_after_a_transaction_was_seen_keeps_the_txid_and_gross_but_no_claim_fields()
    {
        let details = OnchainReceiveDetails {
            fee: None,
            fee_breakdown: None,
            net_credit: None,
            ..receive_details()
        };
        let json =
            serde_json::to_string(&OnchainReceiveDetailsWire::from(&details)).expect("encode");
        let decoded = decode_receive_details(&json).expect("decode");
        assert_eq!(decoded.txid, details.txid);
        assert_eq!(decoded.gross_deposited, details.gross_deposited);
        assert_eq!(decoded.fee, None);
        assert_eq!(decoded.net_credit, None);
    }

    #[test]
    fn every_final_send_state_round_trips() {
        for state in [
            OnchainSendState::Created,
            OnchainSendState::Succeeded { txid: a_txid() },
            OnchainSendState::Refunded {
                reason: "funding tx rejected".to_owned(),
            },
            OnchainSendState::Failed {
                reason: "no transaction produced".to_owned(),
            },
        ] {
            let encoded = encode_send_state(&state).expect("encode");
            assert_eq!(decode_send_state(&encoded).expect("decode"), state);
        }
    }

    #[test]
    fn every_final_receive_state_round_trips() {
        for state in [
            OnchainReceiveState::WaitingForTransaction,
            OnchainReceiveState::WaitingForConfirmation {
                txid: a_txid(),
                gross_deposited: Sats::from_sats(100_000),
            },
            OnchainReceiveState::Confirmed {
                txid: a_txid(),
                gross_deposited: Sats::from_sats(100_000),
            },
            OnchainReceiveState::Claimed {
                txid: a_txid(),
                gross_deposited: Sats::from_sats(100_000),
                net_credit: Amount::from_msats(99_998_500),
            },
            OnchainReceiveState::Failed {
                reason: "claim rejected".to_owned(),
            },
        ] {
            let encoded = encode_receive_state(&state).expect("encode");
            assert_eq!(decode_receive_state(&encoded).expect("decode"), state);
        }
    }

    #[test]
    fn fee_breakdown_wires_round_trip() {
        let breakdown = OnchainSendFeeBreakdown {
            wallet_output: Amount::from_msats(1_200_000),
            funding: Amount::from_msats(34_000),
            change: Amount::from_msats(567),
        };
        let wire = OnchainSendFeeBreakdownWire::from(&breakdown);
        assert_eq!(OnchainSendFeeBreakdown::from(wire), breakdown);

        let breakdown = OnchainReceiveFeeBreakdown {
            peg_in: Amount::from_msats(1_000),
            network_claim: Amount::from_msats(200),
            primary_module: Amount::from_msats(250),
            dust: Amount::from_msats(50),
        };
        let wire = OnchainReceiveFeeBreakdownWire::from(&breakdown);
        assert_eq!(OnchainReceiveFeeBreakdown::from(wire), breakdown);
    }

    #[test]
    fn custom_meta_round_trips_through_from_custom_meta() {
        let wire = OnchainSendDetailsWire::from(&send_details());
        let meta = custom_meta(&wire).expect("encode");
        assert_eq!(
            from_custom_meta::<OnchainSendDetailsWire>(&meta),
            Some(wire)
        );
    }

    #[test]
    fn from_custom_meta_is_none_when_absent_or_malformed() {
        assert_eq!(
            from_custom_meta::<OnchainSendDetailsWire>(&serde_json::Value::Null),
            None
        );
        assert_eq!(
            from_custom_meta::<OnchainSendDetailsWire>(&serde_json::json!({"other": 1})),
            None
        );
    }

    #[test]
    fn a_malformed_record_is_an_internal_error() {
        for json in ["", "{}", r#"{"address":"nope"}"#] {
            assert_eq!(
                decode_send_details(json).expect_err("refused").code,
                ErrorCode::Internal
            );
            assert_eq!(
                decode_receive_details(json).expect_err("refused").code,
                ErrorCode::Internal
            );
        }
        assert_eq!(
            decode_send_state("nonsense").expect_err("refused").code,
            ErrorCode::Internal
        );
    }
}
