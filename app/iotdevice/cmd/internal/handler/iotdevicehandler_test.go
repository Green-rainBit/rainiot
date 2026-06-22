package handler

import (
	"net/http/httptest"
	"strings"
	"testing"

	"rainiot/app/iotdevice/cmd/internal/types"
)

func TestParseIotdeviceRequestWithObjectData(t *testing.T) {
	r := httptest.NewRequest("POST", "/device/connect", strings.NewReader(`{"cmd":"login","data":{"sn":"device-001"}}`))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("ConnId", "device-001")

	var req types.Request
	if err := parseIotdeviceRequest(r, &req); err != nil {
		t.Fatalf("parseIotdeviceRequest() error = %v", err)
	}

	if req.Cmd != "login" {
		t.Fatalf("Cmd = %q, want %q", req.Cmd, "login")
	}
	if string(req.Data) != `{"sn":"device-001"}` {
		t.Fatalf("Data = %s, want object payload", req.Data)
	}
}
