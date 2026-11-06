package errorx

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestIsLogicError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"logic", NewMyError(5000, "device sn cannot be empty"), true},
		{"wrapped logic", NewMyErrorWithCtx(context.Background(), NewMyError(5000, "x")), true},
		{"grpc failed precondition", status.Error(codes.FailedPrecondition, "code:5000"), true},
		{"grpc invalid argument", status.Error(codes.InvalidArgument, "bad request"), true},
		{"grpc unavailable", status.Error(codes.Unavailable, "down"), false},
		{"plain", errors.New("boom"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsLogicError(tc.err); got != tc.want {
				t.Fatalf("IsLogicError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestShouldFallback(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"logic", NewMyError(5000, "x"), false},
		{"canceled", context.Canceled, false},
		{"call error", errors.New("connection refused"), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ShouldFallback(tc.err); got != tc.want {
				t.Fatalf("ShouldFallback(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestHTTPErrorHandler(t *testing.T) {
	code, body := HTTPErrorHandler(NewMyError(5000, "device sn cannot be empty"))
	if code != http.StatusBadRequest {
		t.Fatalf("logic error status = %d, want %d", code, http.StatusBadRequest)
	}
	m, ok := body.(map[string]any)
	if !ok || m["code"] != 5000 || m["msg"] != "device sn cannot be empty" {
		t.Fatalf("unexpected body: %#v", body)
	}

	code, _ = HTTPErrorHandler(errors.New("db down"))
	if code != http.StatusInternalServerError {
		t.Fatalf("call error status = %d, want %d", code, http.StatusInternalServerError)
	}
}
