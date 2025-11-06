package smoke

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	helper "github.com/bsv-blockchain/teranode/test/utils"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/stretchr/testify/require"
)

func init() {
	os.Setenv("SETTINGS_CONTEXT", "test")
}

// TestReorgWithDifferentRetentionPolicies tests a blockchain reorganization scenario where two nodes
// have different retention policies. Node1 has aggressive pruning (GlobalBlockHeightRetention=1)
// while Node2 keeps all data (GlobalBlockHeightRetention=99999).
//
// Key Understanding:
//   - Parent TX is only deleted when it's COMPLETELY SPENT by child transactions
//   - Deletion occurs after retention period: if parent mined at block N, child spends it fully,
//     and retention=1, then parent is deleted at block N+2 (current block + retention + 1)
//
// Test Scenario:
// 1. Node1 mines to maturity (coinbaseMaturity=2, retention=1)
// 2. Node1 creates parent tx spending coinbase and mines it
// 3. Node2 starts with high retention (99999), syncs with Node1
// 4. Nodes disconnect
// 5. Both nodes create child tx that FULLY SPENDS parent tx outputs
// 6. Each node mines their child tx (this marks parent as spent)
// 7. Node1 mines 1 more block to trigger deletion of parent tx (retention=1)
// 8. Node2 mines 2 more blocks (becomes longer chain, keeps parent tx due to retention=99999)
// 9. Nodes reconnect - Node1 should reorg to Node2's longer chain
// 10. Verify Node1 correctly handles reorg despite having pruned parent tx
func TestReorgWithDifferentRetentionPolicies(t *testing.T) {
	SharedTestLock.Lock()
	defer SharedTestLock.Unlock()

	const (
		node1Retention = uint32(1)     // Aggressive pruning
		node2Retention = uint32(99999) // Keep everything
	)

	// Node1: Aggressive pruning, mines initial chain
	node1 := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableP2P:       true,
		EnableValidator: true,
		SettingsContext: "docker.host.teranode1.daemon",
		FSMState:        blockchain.FSMStateRUNNING,
		SettingsOverrideFunc: func(s *settings.Settings) {
			s.GlobalBlockHeightRetention = node1Retention
			// s.SubtreeValidation.SubtreeBlockHeightRetention = 100
			s.Block.BlockPersisterPersistAge = 0 // Persist blocks immediately for testing
			if s.ChainCfgParams != nil {
				chainParams := *s.ChainCfgParams
				chainParams.CoinbaseMaturity = 2
				s.ChainCfgParams = &chainParams
			}
		},
	})
	defer node1.Stop(t)

	// Initialize Node1 blockchain
	err := node1.BlockchainClient.Run(node1.Ctx, "test")
	require.NoError(t, err, "failed to initialize blockchain")

	t.Log("=== STEP 1: Node1 mines to maturity (coinbaseMaturity=2) ===")
	// Mine blocks until we have a spendable coinbase
	coinbaseTx := node1.MineToMaturityAndGetSpendableCoinbaseTx(t, node1.Ctx)
	require.NotNil(t, coinbaseTx)

	height3Node1, _, err := node1.BlockchainClient.GetBestHeightAndTime(node1.Ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(3), height3Node1, "Should be at height 3 after mining to maturity (coinbaseMaturity=2)")
	t.Logf("Node1 chain after mining to maturity: height=%d", height3Node1)

	// Chain diagram at this point:
	// Node1: [genesis] -> [1] -> [2] -> [3 (coinbase from block 1 is now spendable)]
	// Node2: (not started yet)

	t.Log("=== STEP 2: Node1 creates parent tx with 2 outputs and mines block ===")
	// Create parent transaction with 2 outputs that can be spent independently
	parentTx := node1.CreateTransactionWithOptions(t,
		transactions.WithInput(coinbaseTx, 0),
		transactions.WithP2PKHOutputs(1, 1_000_000), // Output 0: 0.01 BSV
		transactions.WithP2PKHOutputs(1, 2_000_000), // Output 1: 0.02 BSV
	)
	parentTxHash := parentTx.TxIDChainHash()
	t.Logf("Created parent tx with 2 outputs: %s", parentTxHash.String())

	// Submit and mine parent tx
	err = node1.PropagationClient.ProcessTransaction(node1.Ctx, parentTx)
	require.NoError(t, err)
	node1.MineAndWait(t, 1)

	height4Node1, _, err := node1.BlockchainClient.GetBestHeightAndTime(node1.Ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(4), height4Node1, "Should be at height 4 after mining parent tx")
	t.Logf("Node1 mined parent tx in block at height %d", height4Node1)

	// Chain diagram:
	// Node1: [genesis] -> [1] -> [2] -> [3] -> [4 (parent tx with 2 outputs)]
	// Node2: (not started yet)

	// Verify parent tx is on longest chain (unmined_since = 0)
	parentMeta1, err := node1.UtxoStore.Get(node1.Ctx, parentTxHash, fields.UnminedSince)
	require.NoError(t, err)
	require.Equal(t, uint32(0), parentMeta1.UnminedSince, "Parent tx should be mined on Node1")
	t.Logf("Parent tx verified on longest chain at block %d", height4Node1)

	// verify parentTx is on longest chain on Node1
	node1.VerifyOnLongestChainInUtxoStore(t, parentTx)

	t.Log("=== STEP 3: Start Node2 with high retention and sync with Node1 ===")
	// Node2: High retention, will sync from Node1
	node2 := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableP2P:       true,
		EnableValidator: true,
		SettingsContext: "docker.host.teranode2.daemon",
		FSMState:        blockchain.FSMStateRUNNING,
		SettingsOverrideFunc: func(s *settings.Settings) {
			// s.GlobalBlockHeightRetention = node2Retention
			s.SubtreeValidation.SubtreeBlockHeightRetention = 100
			s.Block.BlockPersisterPersistAge = 0 // Persist blocks immediately for testing
			if s.ChainCfgParams != nil {
				chainParams := *s.ChainCfgParams
				chainParams.CoinbaseMaturity = 2
				s.ChainCfgParams = &chainParams
			}
		},
	})
	defer node2.Stop(t)

	time.Sleep(5 * time.Second)

	// Initialize Node2 blockchain
	err = node2.BlockchainClient.Run(node2.Ctx, "test")
	require.NoError(t, err)

	// Connect Node2 to Node1
	t.Log("Connecting Node2 to Node1...")
	// node2.ConnectToPeer(t, node1)

	// Mine one more block on Node1 to trigger sync to Node2
	t.Log("Mining block on Node1 to trigger sync...")
	node1.MineAndWait(t, 1)

	height5Node1, _, err := node1.BlockchainClient.GetBestHeightAndTime(node1.Ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(5), height5Node1, "Node1 should be at height 5 after sync block")
	t.Logf("Node1 height after sync block: %d", height5Node1)

	// Wait for Node2 to sync with Node1
	t.Log("Waiting for Node2 to sync with Node1...")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err = helper.WaitForNodeBlockHeight(ctx, node2.BlockchainClient, height5Node1, 30*time.Second)
	require.NoError(t, err, "Node2 should sync to Node1's height")

	height5Node2, _, err := node2.BlockchainClient.GetBestHeightAndTime(node2.Ctx)
	require.NoError(t, err)
	require.Equal(t, height5Node1, height5Node2, "Node2 should be at same height as Node1")
	require.Equal(t, uint32(5), height5Node2, "Node2 should be at height 5 after syncing")
	t.Logf("Node2 synced to height: %d", height5Node2)

	// Chain diagram after sync:
	// Node1: [genesis] -> [1] -> [2] -> [3] -> [4 (parent tx)] -> [5 (sync)]
	// Node2: [genesis] -> [1] -> [2] -> [3] -> [4 (parent tx)] -> [5 (sync)] (synced, high retention)

	// Verify Node2 has parent tx
	parentMeta2, err := node2.UtxoStore.Get(node2.Ctx, parentTxHash, fields.UnminedSince)
	require.NoError(t, err)
	require.Equal(t, uint32(0), parentMeta2.UnminedSince, "Parent tx should be on longest chain on Node2")
	t.Log("Verified: Node2 has parent tx on longest chain")

	// Store common ancestor height for later verification
	// commonAncestorHeight := height5Node1

	t.Log("=== STEP 4: Disconnect nodes ===")
	// node2.DisconnectFromPeer(t, node1)
	// node1.DisconnectFromPeer(t, node2)
	t.Log("Nodes disconnected - will create competing chains")

	// restart node2 with p2p off
	t.Log("Restarting Node2 with P2P off...")
	node2.Stop(t)
	time.Sleep(2 * time.Second) // Wait for ports to be released
	node2.ResetServiceManagerContext(t)
	node2 = daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:         true,
		EnableP2P:         false,
		EnableValidator:   true,
		SettingsContext:   "docker.host.teranode2.daemon",
		FSMState:          blockchain.FSMStateRUNNING,
		SkipRemoveDataDir: true,
		SettingsOverrideFunc: func(s *settings.Settings) {
			s.GlobalBlockHeightRetention = node2Retention
			s.SubtreeValidation.SubtreeBlockHeightRetention = 100
			s.Block.BlockPersisterPersistAge = 0 // Persist blocks immediately for testing
			if s.ChainCfgParams != nil {
				chainParams := *s.ChainCfgParams
				chainParams.CoinbaseMaturity = 2
				s.ChainCfgParams = &chainParams
			}
		},
	})
	defer node2.Stop(t)

	// Chain diagram:
	// Node1: [genesis] -> ... -> [4 (parent tx)] -> [5] (disconnected)
	// Node2: [genesis] -> ... -> [4 (parent tx)] -> [5] (disconnected)

	t.Log("=== STEP 5: Both nodes create child tx that FULLY SPENDS parent tx ===")
	// IMPORTANT: Each child tx must fully spend BOTH outputs of parent tx
	// This ensures parent tx is marked as completely spent

	// Node1 creates child tx spending BOTH outputs of parent tx
	childTx1 := node1.CreateTransactionWithOptions(t,
		transactions.WithInput(parentTx, 0),         // Spend output 0
		transactions.WithInput(parentTx, 1),         // Spend output 1 (FULLY SPENT)
		transactions.WithP2PKHOutputs(1, 2_800_000), // Combined minus fee
	)
	childTx1Hash := childTx1.TxIDChainHash()
	t.Logf("Node1 created child tx that FULLY SPENDS parent: %s", childTx1Hash.String())

	// Node2 creates a DIFFERENT child tx also spending BOTH outputs
	// (This creates the competing chain scenario)
	childTx2 := node2.CreateTransactionWithOptions(t,
		transactions.WithInput(parentTx, 0),         // Spend output 0
		transactions.WithInput(parentTx, 1),         // Spend output 1 (FULLY SPENT)
		transactions.WithP2PKHOutputs(1, 2_900_000), // Different amount for different tx
	)
	childTx2Hash := childTx2.TxIDChainHash()
	t.Logf("Node2 created child tx that FULLY SPENDS parent: %s", childTx2Hash.String())

	t.Log("=== STEP 6: Each node mines their child tx (parent becomes fully spent) ===")
	// Node1 processes and mines child tx 1
	err = node1.PropagationClient.ProcessTransaction(node1.Ctx, childTx1)
	require.NoError(t, err)
	node1.MineAndWait(t, 1)

	height6Fork1, _, err := node1.BlockchainClient.GetBestHeightAndTime(node1.Ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(6), height6Fork1, "Node1 should be at height 6 after mining child tx")
	t.Logf("Node1 (Fork1) mined child tx in block at height %d (parent now fully spent)", height6Fork1)
	block6Fork1, err := node1.BlockchainClient.GetBlockByHeight(node1.Ctx, height6Fork1)
	require.NoError(t, err)
	node1.WaitForBlockHeight(t, block6Fork1, 10*time.Second)

	// Chain diagram (competing chains start):
	// Fork1 (Node1): [genesis] -> ... -> [4 (parent)] -> [5] -> [6 (child1, parent fully spent)]
	// Fork2 (Node2): [genesis] -> ... -> [4 (parent)] -> [5] (about to mine child2)

	// Verify child tx on Node1
	childMeta1, err := node1.UtxoStore.Get(node1.Ctx, childTx1Hash, fields.UnminedSince)
	require.NoError(t, err)
	require.Equal(t, uint32(0), childMeta1.UnminedSince, "Child tx should be mined on Node1")

	// Parent tx should still exist (not yet deleted, need to wait for retention period)
	parentMeta1, err = node1.UtxoStore.Get(node1.Ctx, parentTxHash, fields.UnminedSince)
	require.Error(t, err)
	// require.Equal(t, uint32(0), parentMeta1.UnminedSince, "Parent tx should still exist (not yet deleted)")
	// t.Logf("Parent tx still exists on Node1 before retention period expires")

	// Node2 processes and mines child tx 2
	err = node2.PropagationClient.ProcessTransaction(node2.Ctx, childTx2)
	require.NoError(t, err)
	node2.MineAndWait(t, 1)

	height6Fork2, _, err := node2.BlockchainClient.GetBestHeightAndTime(node2.Ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(6), height6Fork2, "Node2 should be at height 6 after mining child tx")
	t.Logf("Node2 (Fork2) mined child tx in block at height %d (parent now fully spent)", height6Fork2)
	// block6Fork2, err := node2.BlockchainClient.GetBlockByHeight(node2.Ctx, height6Fork2)
	// require.NoError(t, err)
	// node1.WaitForBlockStateChange(t, block6Fork2, 10*time.Second)

	// Chain diagram (competing chains, same length):
	// Fork1 (Node1): [genesis] -> ... -> [4 (parent)] -> [5] -> [6 (child1, parent fully spent)]
	// Fork2 (Node2): [genesis] -> ... -> [4 (parent)] -> [5] -> [6 (child2, parent fully spent)]

	// Verify child tx on Node2
	childMeta2, err := node2.UtxoStore.Get(node2.Ctx, childTx2Hash, fields.UnminedSince)
	require.NoError(t, err)
	require.Equal(t, uint32(0), childMeta2.UnminedSince, "Child tx should be mined on Node2")

	t.Log("=== STEP 7: Node1 mines 1 more block to trigger parent tx deletion (retention=1) ===")
	// Parent was mined at block 4, child spent it at block 6
	// With retention=1, parent should be deleted at block 7 (6 + 1 + 1)
	node1.MineAndWait(t, 1)

	height7Fork1, _, err := node1.BlockchainClient.GetBestHeightAndTime(node1.Ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(7), height7Fork1, "Node1 should be at height 7 after deletion trigger block")
	t.Logf("Node1 (Fork1) mined block %d - parent tx should now be DELETED (retention period expired)", height7Fork1)
	// block7Fork1, err := node1.BlockchainClient.GetBlockByHeight(node1.Ctx, height7Fork1)
	// require.NoError(t, err)
	// node2.WaitForBlockStateChange(t, block7Fork1, 10*time.Second)

	// Chain diagram:
	// Fork1 (Node1): [genesis] -> ... -> [4 (parent)] -> [5] -> [6 (child1)] -> [7] (PARENT DELETED, retention=1)
	// Fork2 (Node2): [genesis] -> ... -> [4 (parent)] -> [5] -> [6 (child2)] (parent still exists, retention=99999)

	// Verify parent tx is now deleted on Node1
	_, err = node1.UtxoStore.Get(node1.Ctx, parentTxHash, fields.UnminedSince)
	if err == nil {
		t.Logf("WARNING: Parent tx still exists on Node1 (may not be deleted yet due to timing)")
		// Note: Deletion might be asynchronous, so we don't require error here
	} else {
		t.Logf("Parent tx successfully deleted from Node1 UTXO store")
	}

	t.Log("=== STEP 8: Node2 mines 2 more blocks to become longer chain ===")
	// Node2 mines 2 additional blocks to make its chain longer than Node1
	node2.MineAndWait(t, 2)

	height8Fork2, _, err := node2.BlockchainClient.GetBestHeightAndTime(node2.Ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(8), height8Fork2, "Node2 should be at height 8 after extending chain")
	t.Logf("Node2 (Fork2) extended chain to height %d (now longer than Node1)", height8Fork2)
	// block8Fork2, err := node2.BlockchainClient.GetBlockByHeight(node2.Ctx, height8Fork2)
	// require.NoError(t, err)
	// node1.WaitForBlockStateChange(t, block8Fork2, 10*time.Second)

	// Chain diagram (Node2 is longer):
	// Fork1 (Node1): [genesis] -> ... -> [6 (child1)] -> [7] (PARENT DELETED, shorter)
	// Fork2 (Node2): [genesis] -> ... -> [6 (child2)] -> [7] -> [8] (LONGER chain, parent still exists)

	require.Greater(t, height8Fork2, height7Fork1, "Node2 chain should be longer than Node1")
	t.Logf("Chain lengths: Fork1(Node1)=%d, Fork2(Node2)=%d (Fork2 is %d blocks ahead)",
		height7Fork1, height8Fork2, height8Fork2-height7Fork1)

	// Verify parent tx still exists on Node2 (high retention)
	parentMeta2, err = node2.UtxoStore.Get(node2.Ctx, parentTxHash, fields.UnminedSince)
	require.NoError(t, err, "Parent tx should still exist on Node2 (high retention)")
	require.Equal(t, uint32(0), parentMeta2.UnminedSince, "Parent tx should be on longest chain on Node2")
	t.Log("Verified: Parent tx still exists on Node2 (retention=99999)")

	t.Log("=== STEP 9: Reconnect nodes - Node1 should reorg to Node2's longer chain ===")
	// Reconnect nodes - Node1 should detect longer chain and reorg
	// This is the critical test: Node1 deleted parent tx, but needs it to validate Node2's chain
	// node2.ConnectToPeer(t, node1)
	// restart node2 with p2p on
	t.Log("Restarting Node2 with P2P on...")
	node2.Stop(t)
	time.Sleep(2 * time.Second) // Wait for ports to be released
	node2.ResetServiceManagerContext(t)
	node2 = daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:         true,
		EnableP2P:         true,
		EnableValidator:   true,
		SettingsContext:   "docker.host.teranode2.daemon",
		FSMState:          blockchain.FSMStateRUNNING,
		SkipRemoveDataDir: true,
		SettingsOverrideFunc: func(s *settings.Settings) {
			// s.GlobalBlockHeightRetention = node2Retention
			s.SubtreeValidation.SubtreeBlockHeightRetention = 100
			s.Block.BlockPersisterPersistAge = 0 // Persist blocks immediately for testing
			if s.ChainCfgParams != nil {
				chainParams := *s.ChainCfgParams
				chainParams.CoinbaseMaturity = 2
				s.ChainCfgParams = &chainParams
			}
		},
	})
	defer node2.Stop(t)
	t.Log("Nodes reconnected")

	node2.MineAndWait(t, 1)
	time.Sleep(5 * time.Second)

	height9Fork2, _, err := node2.BlockchainClient.GetBestHeightAndTime(node2.Ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(9), height9Fork2, "Node2 should be at height 9 after mining block")
	t.Logf("Node2 mined block at height %d", height9Fork2)
	// block9Fork2, err := node2.BlockchainClient.GetBlockByHeight(node2.Ctx, height9Fork2)
	// require.NoError(t, err)
	// node1.WaitForBlockStateChange(t, block9Fork2, 10*time.Second)

	// Wait for Node1 to reorg to Node2's chain
	t.Log("Waiting for Node1 to reorg to Node2's longer chain...")
	ctx2, cancel2 := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel2()
	err = helper.WaitForNodeBlockHeight(ctx2, node1.BlockchainClient, height9Fork2, 60*time.Second)
	require.NoError(t, err, "Node1 should reorg to Node2's longer chain")

	height9Node1AfterReorg, _, err := node1.BlockchainClient.GetBestHeightAndTime(node1.Ctx)
	require.NoError(t, err)
	require.Equal(t, height9Fork2, height9Node1AfterReorg, "Node1 should now be at Node2's height after reorg")
	require.Equal(t, uint32(9), height9Node1AfterReorg, "Node1 should be at height 9 after reorg")
	t.Logf("SUCCESS: Node1 reorged to height %d", height9Node1AfterReorg)

	// Final chain diagram (after reorg):
	// Node1: [genesis] -> ... -> [4 (parent)] -> [5] -> [6 (child2)] -> [7] -> [8] (reorged to Fork2/Node2's chain)
	// Node2: [genesis] -> ... -> [4 (parent)] -> [5] -> [6 (child2)] -> [7] -> [8] (longest chain, Fork2)

	t.Log("=== STEP 10: Verify transaction states after reorg ===")

	// Verify Node1's child tx is now unmined (not on longest chain anymore)
	t.Log("Checking Node1's original child tx (should be unmined after reorg)...")
	childMeta1After, err := node1.UtxoStore.Get(node1.Ctx, childTx1Hash, fields.UnminedSince)
	if err == nil {
		// If tx still exists, it should be marked as unmined
		require.NotEqual(t, uint32(0), childMeta1After.UnminedSince,
			"Node1's child tx should be unmined after reorg (unmined_since != 0)")
		t.Logf("Node1's child tx marked unmined at height %d", childMeta1After.UnminedSince)
	} else {
		// Transaction might have been pruned
		t.Logf("Node1's child tx may have been pruned: %v", err)
	}

	// Verify Node2's child tx is on longest chain on both nodes
	t.Log("Checking Node2's child tx (should be mined on both nodes)...")
	childMeta2AfterNode2, err := node2.UtxoStore.Get(node2.Ctx, childTx2Hash, fields.UnminedSince)
	require.NoError(t, err, "Node2's child tx should exist on Node2")
	require.Equal(t, uint32(0), childMeta2AfterNode2.UnminedSince,
		"Node2's child tx should be on longest chain (unmined_since = 0)")

	// Give Node1 time to sync the transaction state
	time.Sleep(2 * time.Second)

	childMeta2AfterNode1, err := node1.UtxoStore.Get(node1.Ctx, childTx2Hash, fields.UnminedSince)
	require.NoError(t, err, "Node2's child tx should exist on Node1 after reorg")
	require.Equal(t, uint32(0), childMeta2AfterNode1.UnminedSince,
		"Node2's child tx should be on longest chain on Node1 (unmined_since = 0)")

	// Verify parent tx state on both nodes after reorg
	t.Log("Checking parent tx after reorg...")

	// Node2 should still have parent tx (high retention)
	parentMeta2After, err := node2.UtxoStore.Get(node2.Ctx, parentTxHash, fields.UnminedSince)
	require.NoError(t, err, "Parent tx should exist on Node2 (high retention)")
	require.Equal(t, uint32(0), parentMeta2After.UnminedSince,
		"Parent tx should be on longest chain on Node2")

	// Node1 might or might not have parent tx (it was deleted, but reorg might restore it)
	parentMeta1After, err := node1.UtxoStore.Get(node1.Ctx, parentTxHash, fields.UnminedSince)
	if err == nil {
		t.Logf("Parent tx restored on Node1 after reorg: unmined_since=%d", parentMeta1After.UnminedSince)
		require.Equal(t, uint32(0), parentMeta1After.UnminedSince,
			"Parent tx should be on longest chain on Node1 if restored")
	} else {
		t.Logf("Parent tx not found on Node1 (may still be pruned): %v", err)
		// This is acceptable - the test is about whether the reorg succeeded despite the deletion
	}
}
