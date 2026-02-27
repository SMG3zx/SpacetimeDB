package protocol

import "testing"

func TestDecodeInitialConnectionPayload(t *testing.T) {
	valid := InitialConnectionPayload{
		Identity:     "id",
		ConnectionID: "cid",
		Token:        "token",
	}

	t.Run("typed payload", func(t *testing.T) {
		got, err := DecodeInitialConnectionPayload(valid)
		if err != nil {
			t.Fatalf("decode typed payload: %v", err)
		}
		if got != valid {
			t.Fatalf("decoded payload mismatch: got=%+v want=%+v", got, valid)
		}
	})

	t.Run("map payload", func(t *testing.T) {
		got, err := DecodeInitialConnectionPayload(map[string]any{
			"identity":      "id",
			"connection_id": "cid",
			"token":         "token",
		})
		if err != nil {
			t.Fatalf("decode map payload: %v", err)
		}
		if got != valid {
			t.Fatalf("decoded payload mismatch: got=%+v want=%+v", got, valid)
		}
	})

	t.Run("bytes payload", func(t *testing.T) {
		got, err := DecodeInitialConnectionPayload([]byte(`{"identity":"id","connection_id":"cid","token":"token"}`))
		if err != nil {
			t.Fatalf("decode bytes payload: %v", err)
		}
		if got != valid {
			t.Fatalf("decoded payload mismatch: got=%+v want=%+v", got, valid)
		}
	})

	t.Run("invalid payload", func(t *testing.T) {
		if _, err := DecodeInitialConnectionPayload(map[string]any{"identity": "id"}); err == nil {
			t.Fatalf("expected validation error for missing fields")
		}
	})
}

