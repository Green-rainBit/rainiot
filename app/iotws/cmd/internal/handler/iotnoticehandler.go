// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package handler

import (
	"net/http"

	"rainiot/app/iotws/cmd/internal/logic"
	"rainiot/app/iotws/cmd/internal/svc"
	"rainiot/app/iotws/cmd/internal/types"

	"github.com/zeromicro/go-zero/rest/httpx"
)

func IotNoticeHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.Ntice
		if err := httpx.Parse(r, &req); err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
			return
		}

		l := logic.NewIotNoticeLogic(r.Context(), svcCtx)
		err := l.IotNotice(&req)
		if err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
		} else {
			httpx.Ok(w)
		}
	}
}
