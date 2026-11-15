package reapi

import (
	"context"
	"slices"
	"sync/atomic"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// MaxConnections must also cap the listener used with GRPC.
	MaxConnections = 16
	maxActiveRPCs  = 32
	// Leave storage operations for another connection when uploads stall.
	maxStreamsPerConnection = 4
	maxMessageBytes         = MaxBatchBytes + (256 << 10)
	transportWindowBytes    = 64 << 10
	maxHeaderBytes          = 16 << 10
)

// rpcAdmission never queues work. A canceled call retains its slot until the
// decoder/handler returns, even when the transport has already reset its stream.
type rpcAdmission chan struct{}

func (a rpcAdmission) enter(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	select {
	case a <- struct{}{}:
		return nil
	default:
		return status.Error(codes.ResourceExhausted, "too many active RPCs")
	}
}

func (a rpcAdmission) leave() { <-a }

// A receive pump can outlive its streaming handler until gRPC closes the
// transport stream. Keep admission until both owners have finished decoding.
type rpcLease struct {
	admission rpcAdmission
	owners    atomic.Int32
}

func (l *rpcLease) release() {
	if l.owners.Add(-1) == 0 {
		l.admission.leave()
	}
}

type rpcLeaseKey struct{}
type admittedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s admittedStream) Context() context.Context { return s.ctx }

func retainReceiver(ctx context.Context) func() {
	if lease, ok := ctx.Value(rpcLeaseKey{}).(*rpcLease); ok {
		lease.owners.Add(1)
		return lease.release
	}
	// Direct service calls in tests have no transport admission owner.
	return func() {}
}

// grpc-go v1.85 calls MethodDesc.Handler before RecvMsg. Generated handlers
// call dec before the unary interceptor, so admission wraps the descriptor,
// not the interceptor. Clone descriptors: generated globals belong to everyone.
type admittedRegistrar struct {
	server    grpc.ServiceRegistrar
	admission rpcAdmission
}

func (r admittedRegistrar) RegisterService(desc *grpc.ServiceDesc, impl any) {
	admitted := *desc
	admitted.Methods = slices.Clone(desc.Methods)
	for i := range admitted.Methods {
		handler := admitted.Methods[i].Handler
		admitted.Methods[i].Handler = func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
			if err := r.admission.enter(ctx); err != nil {
				return nil, err
			}
			defer r.admission.leave()
			return handler(srv, ctx, dec, interceptor)
		}
	}
	r.server.RegisterService(&admitted, impl)
}
