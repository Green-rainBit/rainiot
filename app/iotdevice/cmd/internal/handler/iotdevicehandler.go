// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package handler

import (
	"encoding/json"
	"net/http"

	"rainiot/app/iotdevice/cmd/internal/logic"
	"rainiot/app/iotdevice/cmd/internal/svc"
	"rainiot/app/iotdevice/cmd/internal/types"

	"github.com/zeromicro/go-zero/rest/httpx"
)

func IotdeviceHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.Request
		if err := parseIotdeviceRequest(r, &req); err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
			return
		}

		l := logic.NewIotdeviceLogic(r.Context(), req.Cmd, svcCtx)
		resp, err := l.Iotdevice(&req)
		if err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
		} else {
			httpx.OkJsonCtx(r.Context(), w, resp)
		}
	}
}

func parseIotdeviceRequest(r *http.Request, req *types.Request) error {
	return json.NewDecoder(r.Body).Decode(req)
}
