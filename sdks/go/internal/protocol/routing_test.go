package protocol

import "testing"

func TestRoutedMessageValidate(t *testing.T) {
	if err := (RoutedMessage{}).Validate(); err == nil {
		t.Fatalf("expected empty kind to fail validation")
	}

	if err := (RoutedMessage{Kind: MessageKindReducerResult}).Validate(); err != nil {
		t.Fatalf("expected non-empty kind to validate, got: %v", err)
	}
}
