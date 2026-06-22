package instances

import (
	"context"
	"sort"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/resolver"
)

type resolvr struct {
	cancelFunc context.CancelFunc
	closeCh    chan struct{}
}

func (r *resolvr) ResolveNow(resolver.ResolveNowOptions) {}

func (r *resolvr) Close() {
	close(r.closeCh)
	r.cancelFunc()
}

func populateEndpoints(ctx context.Context, clientConn resolver.ClientConn, input <-chan []string) {
	for {
		select {
		case cc := <-input:
			connsSet := make(map[string]struct{}, len(cc))
			for _, c := range cc {
				connsSet[c] = struct{}{}
			}
			conns := make([]resolver.Address, 0, len(connsSet))
			for c := range connsSet {
				conns = append(conns, resolver.Address{Addr: c})
			}
			sort.Sort(byAddressString(conns))
			_ = clientConn.UpdateState(resolver.State{Addresses: conns})
		case <-ctx.Done():
			logx.Info("[instances resolver] watch finished")
			return
		}
	}
}

type byAddressString []resolver.Address

func (p byAddressString) Len() int           { return len(p) }
func (p byAddressString) Less(i, j int) bool { return p[i].Addr < p[j].Addr }
func (p byAddressString) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
