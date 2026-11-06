package devicecli

import (
	"context"
	"errors"
	"testing"

	"rainiot/pkg/errorx"
)

type fakeCli struct {
	name  string
	resp  []byte
	err   error
	calls int
}

func (f *fakeCli) Push(ctx context.Context, connId string, message []byte) ([]byte, bool, error) {
	f.calls++
	return f.resp, true, f.err
}

func (f *fakeCli) Information() string { return f.name }

func newTestCli(chain ...DeviceCli) *deviceCli {
	return &deviceCli{chain: chain}
}

func TestPushStopsOnLogicError(t *testing.T) {
	second := &fakeCli{name: "http"}
	first := &fakeCli{name: "grpc", err: errorx.NewMyError(5000, "device sn cannot be empty")}
	d := newTestCli(first, second)

	_, _, err := d.Push(context.Background(), "conn-1", []byte(`{}`))
	if err == nil {
		t.Fatal("expected error")
	}
	if !errorx.IsLogicError(err) {
		t.Fatalf("expected logic error, got %v", err)
	}
	if second.calls != 0 {
		t.Fatalf("expected no fallback, second client called %d times", second.calls)
	}
}

func TestPushFallsBackOnCallError(t *testing.T) {
	second := &fakeCli{name: "http", resp: []byte(`{"ok":true}`)}
	first := &fakeCli{name: "grpc", err: errors.New("connection refused")}
	d := newTestCli(first, second)

	resp, _, err := d.Push(context.Background(), "conn-1", []byte(`{}`))
	if err != nil {
		t.Fatalf("expected fallback success, got %v", err)
	}
	if string(resp) != `{"ok":true}` {
		t.Fatalf("unexpected response: %s", resp)
	}
	if first.calls != 1 || second.calls != 1 {
		t.Fatalf("unexpected call counts: first=%d second=%d", first.calls, second.calls)
	}
}

func TestPushStopsOnContextCanceled(t *testing.T) {
	second := &fakeCli{name: "http"}
	first := &fakeCli{name: "grpc", err: context.Canceled}
	d := newTestCli(first, second)

	_, _, err := d.Push(context.Background(), "conn-1", []byte(`{}`))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if second.calls != 0 {
		t.Fatalf("expected no fallback, second client called %d times", second.calls)
	}
}

func TestPushReturnsLastCallError(t *testing.T) {
	lastErr := errors.New("http down")
	d := newTestCli(
		&fakeCli{name: "grpc", err: errors.New("grpc down")},
		&fakeCli{name: "http", err: lastErr},
	)

	_, _, err := d.Push(context.Background(), "conn-1", []byte(`{}`))
	if !errors.Is(err, lastErr) {
		t.Fatalf("expected last error, got %v", err)
	}
}
