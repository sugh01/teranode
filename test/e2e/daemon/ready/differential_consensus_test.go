package smoke

// TestDifferentialConsensusOracle is the differential consensus oracle
// described in https://github.com/bsv-blockchain/teranode/issues/1413: it
// funds a series of hand-built edge-case transactions with real, mined UTXOs
// and submits each one to both a real SV Node (via test/utils/svnode) and
// Teranode's own RPC, failing the moment the two engines' accept/reject
// verdicts disagree.
//
// Scope of this first cut, and what is deliberately deferred:
//   - Only transactions are exercised; block-level submission (SubmitBlock on
//     both engines) is a DoD extension left for later. The case/build/submit-
//     compare shape below is written so a block-kind case can be added
//     without reshaping the harness.
//   - The seed corpus suggested by the issue (test/consensus/testdata/
//     tx_valid.json / tx_invalid.json) is Bitcoin Core's classic unit-test
//     format: every entry's previous-output is synthetic and does not exist
//     in any real UTXO set. A live node's sendrawtransaction rejects such
//     inputs as missing before ever reaching script/sighash validation, on
//     both engines identically - so replaying that corpus verbatim would be
//     a shallow oracle (it would only catch a node being wrongly lenient
//     about missing inputs) rather than exercising the sighash/opcode edge
//     cases the issue is actually concerned about. This test instead builds
//     a small corpus of edge-case transactions against real funded outputs,
//     so every case genuinely reaches script/policy validation on both
//     engines.
//   - The historic SIGHASH_SINGLE "input index >= number of outputs" bug
//     needs a multi-input transaction and is left as a follow-up case.
import (
	"encoding/hex"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/sighash"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/test"
	helper "github.com/bsv-blockchain/teranode/test/utils"
	"github.com/bsv-blockchain/teranode/test/utils/svnode"
	"github.com/stretchr/testify/require"
)

// differentialCase builds a transaction that spends a single, freshly-funded
// UTXO and exercises one specific edge-case. Each case gets its own dedicated
// UTXO so a divergence (or a mempool-only acceptance) in one case never
// contaminates another.
type differentialCase struct {
	name   string
	amount float64 // funding amount in BSV
	build  func(t *testing.T, utxo *svnode.FundingUTXO, privKey *bec.PrivateKey) *bt.Tx
}

// differentialConsensusFee is subtracted from the funding UTXO for cases that
// don't care about the exact output amount.
const differentialConsensusFee = uint64(1000)

// differentialCases returns the hand-built edge-case transactions submitted
// to both SV Node and Teranode.
func differentialCases(toAddress string) []differentialCase {
	return []differentialCase{
		{
			name:   "sighash_all_forkid",
			amount: 1.0,
			build: func(t *testing.T, utxo *svnode.FundingUTXO, privKey *bec.PrivateKey) *bt.Tx {
				tx := spendUTXO(t, utxo)
				require.NoError(t, tx.AddP2PKHOutputFromAddress(toAddress, utxo.Amount-differentialConsensusFee))
				signInputWithSighash(t, tx, 0, privKey, sighash.AllForkID, false)
				return tx
			},
		},
		{
			// SIGHASH_NONE leaves every output uncommitted; both engines must
			// still treat it as a legal, standard signature type.
			name:   "sighash_none_forkid",
			amount: 1.0,
			build: func(t *testing.T, utxo *svnode.FundingUTXO, privKey *bec.PrivateKey) *bt.Tx {
				tx := spendUTXO(t, utxo)
				require.NoError(t, tx.AddP2PKHOutputFromAddress(toAddress, utxo.Amount-differentialConsensusFee))
				signInputWithSighash(t, tx, 0, privKey, sighash.NoneForkID, false)
				return tx
			},
		},
		{
			// SIGHASH_SINGLE with the output count equal to the input count -
			// the well-defined, non-buggy use of the flag.
			name:   "sighash_single_forkid",
			amount: 1.0,
			build: func(t *testing.T, utxo *svnode.FundingUTXO, privKey *bec.PrivateKey) *bt.Tx {
				tx := spendUTXO(t, utxo)
				require.NoError(t, tx.AddP2PKHOutputFromAddress(toAddress, utxo.Amount-differentialConsensusFee))
				signInputWithSighash(t, tx, 0, privKey, sighash.SingleForkID, false)
				return tx
			},
		},
		{
			name:   "sighash_all_forkid_anyonecanpay",
			amount: 1.0,
			build: func(t *testing.T, utxo *svnode.FundingUTXO, privKey *bec.PrivateKey) *bt.Tx {
				tx := spendUTXO(t, utxo)
				require.NoError(t, tx.AddP2PKHOutputFromAddress(toAddress, utxo.Amount-differentialConsensusFee))
				signInputWithSighash(t, tx, 0, privKey, sighash.AllForkID|sighash.AnyOneCanPay, false)
				return tx
			},
		},
		{
			// 1 satoshi is Teranode's configured DustLimit
			// (services/validator/Validator.go). SV Node applies its own,
			// independently configured dust-relay policy, so this case
			// probes whether the two engines actually agree at that
			// boundary rather than assuming they do.
			name:   "dust_output",
			amount: 1.0,
			build: func(t *testing.T, utxo *svnode.FundingUTXO, privKey *bec.PrivateKey) *bt.Tx {
				tx := spendUTXO(t, utxo)

				dustScript, err := bscript.NewP2PKHFromAddress(toAddress)
				require.NoError(t, err)

				// Keep the fee ordinary and isolate the dust-boundary
				// behaviour: a genuine change output alongside one
				// deliberately dust-sized (1 sat) output. A single 1-sat
				// output with no change would instead leave almost the
				// entire input as an absurdly-high fee, which SV Node
				// rejects as a fee sanity-check, not a dust one.
				tx.AddOutput(&bt.Output{Satoshis: 1, LockingScript: dustScript})
				require.NoError(t, tx.AddP2PKHOutputFromAddress(toAddress, utxo.Amount-1-differentialConsensusFee))
				signInputWithSighash(t, tx, 0, privKey, sighash.AllForkID, false)

				return tx
			},
		},
		{
			// Unlocking script pushes the signature and pubkey via
			// OP_PUSHDATA1 even though both are short enough for a direct
			// push, deliberately violating the MINIMALDATA rule.
			name:   "non_minimal_pushdata",
			amount: 1.0,
			build: func(t *testing.T, utxo *svnode.FundingUTXO, privKey *bec.PrivateKey) *bt.Tx {
				tx := spendUTXO(t, utxo)
				require.NoError(t, tx.AddP2PKHOutputFromAddress(toAddress, utxo.Amount-differentialConsensusFee))
				signInputWithSighash(t, tx, 0, privKey, sighash.AllForkID, true)
				return tx
			},
		},
		{
			// Deliberately larger than the classic bitcoind-family 223-byte
			// datacarriersize default. Neither engine's configured limit was
			// verified ahead of time, so this is a boundary probe rather
			// than a targeted regression check for a known-different value.
			name:   "oversized_op_return",
			amount: 1.0,
			build: func(t *testing.T, utxo *svnode.FundingUTXO, privKey *bec.PrivateKey) *bt.Tx {
				tx := spendUTXO(t, utxo)
				tx.AddOutput(&bt.Output{
					Satoshis:      0,
					LockingScript: opReturnLockingScript(t, make([]byte, 1000)),
				})
				require.NoError(t, tx.AddP2PKHOutputFromAddress(toAddress, utxo.Amount-differentialConsensusFee))
				signInputWithSighash(t, tx, 0, privKey, sighash.AllForkID, false)
				return tx
			},
		},
		{
			// Bare (non-P2SH) CHECKMULTISIG output - the "non-standard-but-
			// valid script" example named in the issue.
			name:   "bare_multisig_output",
			amount: 1.0,
			build: func(t *testing.T, utxo *svnode.FundingUTXO, privKey *bec.PrivateKey) *bt.Tx {
				tx := spendUTXO(t, utxo)
				tx.AddOutput(&bt.Output{
					Satoshis:      utxo.Amount - differentialConsensusFee,
					LockingScript: bareMultisigLockingScript(t, privKey.PubKey().Compressed()),
				})
				signInputWithSighash(t, tx, 0, privKey, sighash.AllForkID, false)
				return tx
			},
		},
	}
}

// spendUTXO starts a new transaction spending the given confirmed funding
// UTXO. The caller is responsible for adding outputs and signing.
func spendUTXO(t *testing.T, utxo *svnode.FundingUTXO) *bt.Tx {
	tx := bt.NewTx()

	err := tx.FromUTXOs(&bt.UTXO{
		TxIDHash:      utxo.Tx.TxIDChainHash(),
		Vout:          utxo.Vout,
		LockingScript: utxo.LockingScript,
		Satoshis:      utxo.Amount,
	})
	require.NoError(t, err)

	return tx
}

// signInputWithSighash signs a P2PKH input with the given sighash flag.
// When nonMinimalPush is true, the signature and pubkey are pushed via
// OP_PUSHDATA1 instead of a minimal-length push, to probe MINIMALDATA
// standardness/consensus divergence between the two engines.
func signInputWithSighash(t *testing.T, tx *bt.Tx, inputIndex int, privKey *bec.PrivateKey, flag sighash.Flag, nonMinimalPush bool) {
	sigHash, err := tx.CalcInputSignatureHash(uint32(inputIndex), flag)
	require.NoError(t, err)

	sig, err := privKey.Sign(sigHash)
	require.NoError(t, err)

	sigBytes := append(sig.Serialize(), byte(flag))
	pubKeyBytes := privKey.PubKey().Compressed()

	unlockScript := &bscript.Script{}

	if nonMinimalPush {
		appendNonMinimalPush(unlockScript, sigBytes)
		appendNonMinimalPush(unlockScript, pubKeyBytes)
	} else {
		require.NoError(t, unlockScript.AppendPushData(sigBytes))
		require.NoError(t, unlockScript.AppendPushData(pubKeyBytes))
	}

	tx.Inputs[inputIndex].UnlockingScript = unlockScript
}

// appendNonMinimalPush appends data via OP_PUSHDATA1 regardless of length,
// deliberately violating the MINIMALDATA rule.
func appendNonMinimalPush(s *bscript.Script, data []byte) {
	*s = append(*s, bscript.OpPUSHDATA1, byte(len(data)))
	*s = append(*s, data...)
}

// opReturnLockingScript builds a provably-unspendable data-carrier output.
func opReturnLockingScript(t *testing.T, data []byte) *bscript.Script {
	s := &bscript.Script{}
	require.NoError(t, s.AppendOpcodes(bscript.OpRETURN))
	require.NoError(t, s.AppendPushData(data))

	return s
}

// bareMultisigLockingScript builds a 1-of-1 CHECKMULTISIG output paying to
// pubKey directly, i.e. not wrapped in P2SH.
func bareMultisigLockingScript(t *testing.T, pubKey []byte) *bscript.Script {
	s := &bscript.Script{}
	require.NoError(t, s.AppendOpcodes(bscript.Op1))
	require.NoError(t, s.AppendPushData(pubKey))
	require.NoError(t, s.AppendOpcodes(bscript.Op1, bscript.OpCHECKMULTISIG))

	return s
}

func TestDifferentialConsensusOracle(t *testing.T) {
	legacySyncTestLock.Lock()
	defer legacySyncTestLock.Unlock()

	sv := newSVNode()

	ctx := t.Context()
	require.NoError(t, sv.Start(ctx), errStartSVNode)

	defer func() { _ = sv.Stop(ctx) }()

	// Mature at least one coinbase (regtest COINBASE_MATURITY=100) before
	// starting Teranode, so the wallet used to fund test cases has a
	// spendable balance. Generating up front, before Teranode connects,
	// also makes Teranode's initial sync an ordinary IBD rather than a
	// live catch-up - the more reliable path (see legacy_sync_test.go).
	_, err := sv.Generate(101)
	require.NoError(t, err, "failed to mature initial coinbase on SV Node")

	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableP2P:       true,
		EnableLegacy:    true,
		EnableValidator: true,
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
			func(s *settings.Settings) {
				s.Legacy.ConnectPeers = []string{sv.P2PHost()}
				s.P2P.StaticPeers = []string{}
			},
		),
		FSMState: blockchain.FSMStateRUNNING,
	})
	defer td.Stop(t)

	initialHeight, err := sv.GetBlockCount()
	require.NoError(t, err)
	require.NoError(t, helper.WaitForNodeBlockHeight(ctx, td.BlockchainClient, uint32(initialHeight), 120*time.Second),
		"Teranode should sync initial blocks via IBD")

	privKey := td.GetPrivateKey(t)

	txCreator, err := svnode.NewTxCreator(sv, privKey)
	require.NoError(t, err)

	for _, tc := range differentialCases(txCreator.Address()) {
		t.Run(tc.name, func(t *testing.T) {
			utxo, err := txCreator.CreateConfirmedFunding(tc.amount)
			require.NoError(t, err, "failed to fund case %q on SV Node", tc.name)

			height, err := sv.GetBlockCount()
			require.NoError(t, err)
			require.NoError(t, helper.WaitForNodeBlockHeight(ctx, td.BlockchainClient, uint32(height), 60*time.Second),
				"Teranode should sync the funding block for case %q", tc.name)

			tx := tc.build(t, utxo, privKey)

			_, svErr := sv.SendRawTransaction(tx.String())
			svAccepted := svErr == nil

			_, tdErr := td.CallRPC(ctx, "sendrawtransaction", []any{hex.EncodeToString(tx.ExtendedBytes())})
			tdAccepted := tdErr == nil

			require.Equalf(t, svAccepted, tdAccepted,
				"consensus divergence in case %q: SV Node accepted=%v (err=%v), Teranode accepted=%v (err=%v)",
				tc.name, svAccepted, svErr, tdAccepted, tdErr)

			t.Logf("case %q: SV Node accepted=%v, Teranode accepted=%v", tc.name, svAccepted, tdAccepted)
		})
	}

	// Capstone: mine whatever the two engines agreed to accept and confirm
	// they land on the same chain tip (issue's optional resulting-state
	// equality check).
	_, err = sv.Generate(1)
	require.NoError(t, err, "failed to mine capstone block on SV Node")

	height, err := sv.GetBlockCount()
	require.NoError(t, err)
	require.NoError(t, helper.WaitForNodeBlockHeight(ctx, td.BlockchainClient, uint32(height), 60*time.Second))

	svHash, err := sv.GetBestBlockHash()
	require.NoError(t, err)

	tdHeader, _, err := td.BlockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)

	require.Equal(t, svHash, tdHeader.Hash().String(), "chain tip mismatch after mining differential-oracle transactions")
}
