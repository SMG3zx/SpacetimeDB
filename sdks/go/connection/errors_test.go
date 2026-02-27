package connection

import (
	"errors"
	"testing"
)

func TestIsCodeMatchesWrappedError(t *testing.T) {
	err := wrapError(ErrorSendFailed, "send_call_reducer", errors.New("boom"))
	if !IsCode(err, ErrorSendFailed) {
		t.Fatalf("expected IsCode to match ErrorSendFailed")
	}
	if IsCode(err, ErrorEncodeFailed) {
		t.Fatalf("did not expect IsCode to match ErrorEncodeFailed")
	}
}
