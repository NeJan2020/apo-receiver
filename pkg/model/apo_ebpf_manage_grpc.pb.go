// ebpf manage proto
// must sync with ebpf-agent/protos/kindling_ebpf_manage.proto

// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.3.0
// - protoc             v3.20.3
// source: pkg/model/apo_ebpf_manage.proto

package model

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

const (
	FileService_GetFile_FullMethodName = "/kindling.FileService/GetFile"
)

// FileServiceClient is the client API for FileService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type FileServiceClient interface {
	GetFile(ctx context.Context, in *FileRequest, opts ...grpc.CallOption) (*FileResponse, error)
}

type fileServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewFileServiceClient(cc grpc.ClientConnInterface) FileServiceClient {
	return &fileServiceClient{cc}
}

func (c *fileServiceClient) GetFile(ctx context.Context, in *FileRequest, opts ...grpc.CallOption) (*FileResponse, error) {
	out := new(FileResponse)
	err := c.cc.Invoke(ctx, FileService_GetFile_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// FileServiceServer is the server API for FileService service.
// All implementations must embed UnimplementedFileServiceServer
// for forward compatibility
type FileServiceServer interface {
	GetFile(context.Context, *FileRequest) (*FileResponse, error)
	mustEmbedUnimplementedFileServiceServer()
}

// UnimplementedFileServiceServer must be embedded to have forward compatible implementations.
type UnimplementedFileServiceServer struct {
}

func (UnimplementedFileServiceServer) GetFile(context.Context, *FileRequest) (*FileResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetFile not implemented")
}
func (UnimplementedFileServiceServer) mustEmbedUnimplementedFileServiceServer() {}

// UnsafeFileServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to FileServiceServer will
// result in compilation errors.
type UnsafeFileServiceServer interface {
	mustEmbedUnimplementedFileServiceServer()
}

func RegisterFileServiceServer(s grpc.ServiceRegistrar, srv FileServiceServer) {
	s.RegisterService(&FileService_ServiceDesc, srv)
}

func _FileService_GetFile_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(FileRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(FileServiceServer).GetFile(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: FileService_GetFile_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(FileServiceServer).GetFile(ctx, req.(*FileRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// FileService_ServiceDesc is the grpc.ServiceDesc for FileService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var FileService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "kindling.FileService",
	HandlerType: (*FileServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "GetFile",
			Handler:    _FileService_GetFile_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "pkg/model/apo_ebpf_manage.proto",
}
