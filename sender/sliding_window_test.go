package sender

import (
	"sync"
	"testing"
)

func TestSWStoreAndLoad(t *testing.T) {
	sw := NewSlidingWindow(100)
	data := []byte("hello world")
	sw.Store(0, data)

	got, ok := sw.Load(0)
	if !ok {
		t.Fatal("Load(0) returned false")
	}
	if string(got) != "hello world" {
		t.Fatalf("Load(0) = %q, want %q", got, "hello world")
	}
}

func TestSWStoreIsCopy(t *testing.T) {
	sw := NewSlidingWindow(100)
	data := []byte("original")
	sw.Store(0, data)

	// Mutate the original slice — stored copy must be unaffected.
	data[0] = 'X'
	got, _ := sw.Load(0)
	if got[0] == 'X' {
		t.Fatal("Store did not copy: mutation of source affected stored data")
	}
}

func TestSWLoadNilSlot(t *testing.T) {
	sw := NewSlidingWindow(100)
	_, ok := sw.Load(42)
	if ok {
		t.Fatal("Load of empty slot should return false")
	}
}

func TestSWAdvanceEvictsSlots(t *testing.T) {
	sw := NewSlidingWindow(100)
	sw.Store(0, []byte("a"))
	sw.Store(1, []byte("b"))
	sw.Store(2, []byte("c"))

	sw.Advance(1) // evict slots 0 and 1

	if _, ok := sw.Load(0); ok {
		t.Fatal("slot 0 should be evicted after Advance(1)")
	}
	if _, ok := sw.Load(1); ok {
		t.Fatal("slot 1 should be evicted after Advance(1)")
	}
	if _, ok := sw.Load(2); !ok {
		t.Fatal("slot 2 should still be available after Advance(1)")
	}
}

func TestSWAdvanceZeroNoOp(t *testing.T) {
	sw := NewSlidingWindow(100)
	sw.Store(0, []byte("keep"))

	sw.Advance(0) // explicit no-op per spec

	if _, ok := sw.Load(0); !ok {
		t.Fatal("Advance(0) should be a no-op; slot 0 should remain")
	}
}

func TestSWAdvanceIdempotent(t *testing.T) {
	sw := NewSlidingWindow(100)
	sw.Store(5, []byte("data"))

	sw.Advance(3)
	sw.Advance(2) // lower than previous HC — should be a no-op
	sw.Advance(3) // same as previous HC — should be a no-op

	if _, ok := sw.Load(5); !ok {
		t.Fatal("repeated/backward Advance should not evict beyond original HC")
	}
}

func TestSWIsFullPreventsWraparound(t *testing.T) {
	sw := NewSlidingWindow(10)

	// Fill slots 0-9
	for i := uint64(0); i < 10; i++ {
		if sw.IsFull(i) {
			t.Fatalf("slot %d should not be full before window fills", i)
		}
		sw.Store(i, []byte{byte(i)})
	}

	// Slot 10 requires HC to advance — should be full
	if !sw.IsFull(10) {
		t.Fatal("slot 10 should be full (window size 10, HC unset)")
	}

	// Advance(0) is a documented no-op to preserve seq-0 retransmit.
	// Advance HC to 1: evicts slots 0,1 → window now holds seq [2..11].
	sw.Advance(1)
	if sw.IsFull(11) {
		t.Fatal("slot 11 should be available after Advance(1)")
	}
	// IsFull(12) = 12 >= hc+size+1 = 1+10+1 = 12 → true
	if !sw.IsFull(12) {
		t.Fatal("slot 12 should be full (window=[2..11])")
	}
}

func TestSWWraparoundCorrectness(t *testing.T) {
	sw := NewSlidingWindow(10)

	// Store slots 0-9, then advance HC so slots can be reused.
	for i := uint64(0); i < 10; i++ {
		sw.Store(i, []byte{byte(i)})
	}
	sw.Advance(4) // evict 0-4, free 5 slots

	// Store 10-14, which reuse physical slots 0-4
	for i := uint64(10); i < 15; i++ {
		sw.Store(i, []byte{byte(i)})
	}

	// Old evicted slots should not be loadable
	for i := uint64(0); i <= 4; i++ {
		if _, ok := sw.Load(i); ok {
			t.Fatalf("evicted slot %d should not be loadable", i)
		}
	}

	// Slots 5-9 still hold original data
	for i := uint64(5); i < 10; i++ {
		got, ok := sw.Load(i)
		if !ok {
			t.Fatalf("slot %d should still be available", i)
		}
		if got[0] != byte(i) {
			t.Fatalf("slot %d: got %d, want %d", i, got[0], i)
		}
	}

	// New slots 10-14 hold new data
	for i := uint64(10); i < 15; i++ {
		got, ok := sw.Load(i)
		if !ok {
			t.Fatalf("slot %d should be available", i)
		}
		if got[0] != byte(i) {
			t.Fatalf("slot %d: got %d, want %d", i, got[0], i)
		}
	}
}

func TestSWConcurrentAccess(t *testing.T) {
	sw := NewSlidingWindow(1000)

	var wg sync.WaitGroup

	// Writer goroutine: stores sequential data
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := uint64(0); i < 500; i++ {
			sw.Store(i, []byte{byte(i % 256)})
		}
	}()

	// Reader goroutine: loads and advances
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := uint64(0); i < 250; i++ {
			sw.Load(i)
			if i > 0 && i%50 == 0 {
				sw.Advance(i - 1)
			}
		}
	}()

	// IsFull checker goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := uint64(0); i < 500; i++ {
			sw.IsFull(i)
		}
	}()

	wg.Wait() // no panic = pass
}

func TestSWLoadAfterEvictionReturnsNil(t *testing.T) {
	sw := NewSlidingWindow(windowSize) // use production window size

	// Store at seq 100, advance past it, try to load
	sw.Store(100, []byte("data"))
	sw.Advance(200)

	if _, ok := sw.Load(100); ok {
		t.Fatal("Load should return false for seq below HC")
	}
	if _, ok := sw.Load(200); ok {
		t.Fatal("Load should return false for seq at HC")
	}
}
