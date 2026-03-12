package client_test

import (
	"sync"
	"testing"

	"github.com/clockworklabs/spacetimedb-go/client"
)

func TestCallbackRegisterInvokeRemove(t *testing.T) {
	r := client.NewCallbackRegistry[string, int]()

	var mu sync.Mutex
	var results []int

	id1 := r.Register(func(ctx string, arg int) {
		mu.Lock()
		results = append(results, arg)
		mu.Unlock()
	})
	id2 := r.Register(func(ctx string, arg int) {
		mu.Lock()
		results = append(results, arg*10)
		mu.Unlock()
	})

	r.Invoke("ctx", 5)
	if len(results) != 2 {
		t.Fatalf("expected 2 callbacks fired, got %d", len(results))
	}

	// Remove one and verify only one fires
	r.Remove(id1)
	results = results[:0]
	r.Invoke("ctx", 3)
	if len(results) != 1 || results[0] != 30 {
		t.Fatalf("expected [30], got %v", results)
	}

	r.Remove(id2)
	results = results[:0]
	r.Invoke("ctx", 7)
	if len(results) != 0 {
		t.Fatalf("expected no callbacks, got %v", results)
	}
}

func TestCallbackIdUnique(t *testing.T) {
	r := client.NewCallbackRegistry[string, int]()
	ids := make(map[client.CallbackId]bool)
	for i := 0; i < 100; i++ {
		id := r.Register(func(string, int) {})
		if ids[id] {
			t.Fatalf("duplicate callback id %d", id)
		}
		ids[id] = true
	}
}

func TestCallbackConcurrentSafe(t *testing.T) {
	r := client.NewCallbackRegistry[string, int]()
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id := r.Register(func(string, int) {})
			r.Invoke("ctx", 1)
			r.Remove(id)
		}()
	}
	wg.Wait()
}
