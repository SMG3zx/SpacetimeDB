package subscription

import (
	"testing"

	"github.com/SMG3zx/SpacetimeDB/sdks/go/internal/protocol"
)

func TestIsExpectedMessageKind(t *testing.T) {
	cases := []struct {
		kind protocol.MessageKind
		want bool
	}{
		{kind: protocol.MessageKindSubscribeApplied, want: true},
		{kind: protocol.MessageKindTransactionUpdate, want: true},
		{kind: protocol.MessageKindSubscriptionError, want: true},
		{kind: protocol.MessageKindUnsubscribeApplied, want: true},
		{kind: protocol.MessageKindReducerResult, want: false},
	}

	for _, tc := range cases {
		if got := IsExpectedMessageKind(tc.kind); got != tc.want {
			t.Fatalf("unexpected expected-kind value for %q: got %v want %v", tc.kind, got, tc.want)
		}
	}
}

func TestIsTerminalMessageKind(t *testing.T) {
	if !IsTerminalMessageKind(protocol.MessageKindSubscriptionError) {
		t.Fatalf("subscription_error should be terminal")
	}
	if !IsTerminalMessageKind(protocol.MessageKindUnsubscribeApplied) {
		t.Fatalf("unsubscribe_applied should be terminal")
	}
	if IsTerminalMessageKind(protocol.MessageKindTransactionUpdate) {
		t.Fatalf("transaction_update should not be terminal")
	}
}
