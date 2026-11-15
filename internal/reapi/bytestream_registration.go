package reapi

import (
	"context"

	bspb "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
)

// The pinned ByteStream generator accepts only *grpc.Server and hides its
// descriptor. This small typed registration adapter lets QueryWriteStatus use
// the same predecode admission as the modern REAPI generated descriptors.
func registerByteStream(registrar grpc.ServiceRegistrar, server bspb.ByteStreamServer) {
	registrar.RegisterService(&grpc.ServiceDesc{
		ServiceName: "google.bytestream.ByteStream",
		HandlerType: (*bspb.ByteStreamServer)(nil),
		Methods: []grpc.MethodDesc{{MethodName: "QueryWriteStatus", Handler: func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
			request := new(bspb.QueryWriteStatusRequest)
			if err := dec(request); err != nil {
				return nil, err
			}
			handler := func(ctx context.Context, request any) (any, error) {
				return srv.(bspb.ByteStreamServer).QueryWriteStatus(ctx, request.(*bspb.QueryWriteStatusRequest))
			}
			if interceptor != nil {
				return interceptor(ctx, request, &grpc.UnaryServerInfo{Server: srv, FullMethod: "/google.bytestream.ByteStream/QueryWriteStatus"}, handler)
			}
			return handler(ctx, request)
		}}},
		Streams: []grpc.StreamDesc{
			{StreamName: "Read", ServerStreams: true, Handler: func(srv any, stream grpc.ServerStream) error {
				request := new(bspb.ReadRequest)
				if err := stream.RecvMsg(request); err != nil {
					return err
				}
				return srv.(bspb.ByteStreamServer).Read(request, &grpc.GenericServerStream[bspb.ReadRequest, bspb.ReadResponse]{ServerStream: stream})
			}},
			{StreamName: "Write", ClientStreams: true, Handler: func(srv any, stream grpc.ServerStream) error {
				return srv.(bspb.ByteStreamServer).Write(&grpc.GenericServerStream[bspb.WriteRequest, bspb.WriteResponse]{ServerStream: stream})
			}},
		},
		Metadata: "google/bytestream/bytestream.proto",
	}, server)
}
