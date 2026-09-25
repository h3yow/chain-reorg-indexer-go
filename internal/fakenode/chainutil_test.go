package fakenode

import "testing"

func TestFakeNodeServesCanonicalChain(t *testing.T) {
	chain := BuildChain(0, 5, "", "a")
	n := New(chain)
	defer n.Close()

	client := MakeTestClient(t, MakeClientCfg(n.URL()))
	defer client.Close()

	head, err := client.GetChainHead()
	if err != nil {
		t.Fatalf("head: %v", err)
	}
	if head.Number != 4 || head.Hash != "0xa-4" {
		t.Fatalf("head = %+v", head)
	}
	b, err := client.GetBlockByNumber(2)
	if err != nil {
		t.Fatalf("block 2: %v", err)
	}
	if b.Hash != "0xa-2" || b.ParentHash != "0xa-1" || len(b.Events) != 1 {
		t.Fatalf("block 2 = %+v", b)
	}
}

// TestFakeNodeReorgSwapsCanonical verifies SetReorg repoints the tail.
func TestFakeNodeReorgSwapsCanonical(t *testing.T) {
	chain := BuildChain(0, 6, "", "a") // 0..5
	n := New(chain)
	defer n.Close()

	fork := BuildChain(4, 3, n.CanonicalHash(3), "b")
	n.SetReorg(3, fork)

	if n.CanonicalHeight() != 6 {
		t.Fatalf("canonical height = %d, want 6", n.CanonicalHeight())
	}
	if n.CanonicalHash(4) != "0xb-4" {
		t.Fatalf("height 4 hash = %s, want 0xb-4", n.CanonicalHash(4))
	}
}
