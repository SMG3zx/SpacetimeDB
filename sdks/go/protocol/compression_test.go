package protocol_test

import (
	"bytes"
	"compress/gzip"
	"testing"

	"github.com/andybalholm/brotli"
	"github.com/clockworklabs/spacetimedb-go/protocol"
)

func TestDecompressNone(t *testing.T) {
	payload := []byte("hello world")
	frame := append([]byte{0x00}, payload...)
	got, err := protocol.DecompressServerMessage(frame)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(payload) {
		t.Fatalf("got %q, want %q", got, payload)
	}
}

func TestDecompressGzip(t *testing.T) {
	payload := []byte("hello gzip world")
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	w.Write(payload)
	w.Close()
	frame := append([]byte{0x02}, buf.Bytes()...)
	got, err := protocol.DecompressServerMessage(frame)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(payload) {
		t.Fatalf("got %q, want %q", got, payload)
	}
}

func TestDecompressBrotli(t *testing.T) {
	payload := []byte("hello brotli world")
	var buf bytes.Buffer
	w := brotli.NewWriter(&buf)
	w.Write(payload)
	w.Close()
	frame := append([]byte{0x01}, buf.Bytes()...)
	got, err := protocol.DecompressServerMessage(frame)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(payload) {
		t.Fatalf("got %q, want %q", got, payload)
	}
}

func TestDecompressUnknown(t *testing.T) {
	_, err := protocol.DecompressServerMessage([]byte{0xFF, 0x00})
	if err == nil {
		t.Fatal("expected error for unknown tag")
	}
}

func TestDecompressEmpty(t *testing.T) {
	_, err := protocol.DecompressServerMessage([]byte{})
	if err == nil {
		t.Fatal("expected error for empty frame")
	}
}
