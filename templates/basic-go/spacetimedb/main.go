package main

import (
	"log"
	"sync"
)

// NOTE: This file is a scaffold for future Go server bindings.
// It is not currently publishable as a SpacetimeDB server module.

// Person mirrors the basic Rust template's table row shape.
type Person struct {
	Name string
}

// ReducerContext is a placeholder type for future Go server bindings.
type ReducerContext struct{}

var (
	mu     sync.RWMutex
	people []Person
)

// Init is called when the module is initially published.
func Init(_ *ReducerContext) {
	// Called when the module is initially published
}

// IdentityConnected is called every time a new client connects.
func IdentityConnected(_ *ReducerContext) {
	// Called every time a new client connects
}

// IdentityDisconnected is called every time a client disconnects.
func IdentityDisconnected(_ *ReducerContext) {
	// Called every time a client disconnects
}

func Add(_ *ReducerContext, name string) {
	mu.Lock()
	defer mu.Unlock()
	people = append(people, Person{Name: name})
}

func SayHello(_ *ReducerContext) {
	mu.RLock()
	defer mu.RUnlock()

	for _, person := range people {
		log.Printf("Hello, %s!", person.Name)
	}
	log.Printf("Hello, World!")
}

func main() {}
