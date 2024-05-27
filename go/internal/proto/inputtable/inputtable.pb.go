//
// Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending

// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.28.1
// 	protoc        v3.21.2
// source: deephaven/proto/inputtable.proto

package inputtable

import (
	_ "github.com/deephaven/deephaven-core/go/internal/proto/extensions"
	ticket "github.com/deephaven/deephaven-core/go/internal/proto/ticket"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type AddTableRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	InputTable *ticket.Ticket `protobuf:"bytes,1,opt,name=input_table,json=inputTable,proto3" json:"input_table,omitempty"`
	TableToAdd *ticket.Ticket `protobuf:"bytes,2,opt,name=table_to_add,json=tableToAdd,proto3" json:"table_to_add,omitempty"`
}

func (x *AddTableRequest) Reset() {
	*x = AddTableRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_deephaven_proto_inputtable_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *AddTableRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*AddTableRequest) ProtoMessage() {}

func (x *AddTableRequest) ProtoReflect() protoreflect.Message {
	mi := &file_deephaven_proto_inputtable_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use AddTableRequest.ProtoReflect.Descriptor instead.
func (*AddTableRequest) Descriptor() ([]byte, []int) {
	return file_deephaven_proto_inputtable_proto_rawDescGZIP(), []int{0}
}

func (x *AddTableRequest) GetInputTable() *ticket.Ticket {
	if x != nil {
		return x.InputTable
	}
	return nil
}

func (x *AddTableRequest) GetTableToAdd() *ticket.Ticket {
	if x != nil {
		return x.TableToAdd
	}
	return nil
}

type AddTableResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *AddTableResponse) Reset() {
	*x = AddTableResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_deephaven_proto_inputtable_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *AddTableResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*AddTableResponse) ProtoMessage() {}

func (x *AddTableResponse) ProtoReflect() protoreflect.Message {
	mi := &file_deephaven_proto_inputtable_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use AddTableResponse.ProtoReflect.Descriptor instead.
func (*AddTableResponse) Descriptor() ([]byte, []int) {
	return file_deephaven_proto_inputtable_proto_rawDescGZIP(), []int{1}
}

type DeleteTableRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	InputTable    *ticket.Ticket `protobuf:"bytes,1,opt,name=input_table,json=inputTable,proto3" json:"input_table,omitempty"`
	TableToRemove *ticket.Ticket `protobuf:"bytes,2,opt,name=table_to_remove,json=tableToRemove,proto3" json:"table_to_remove,omitempty"`
}

func (x *DeleteTableRequest) Reset() {
	*x = DeleteTableRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_deephaven_proto_inputtable_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *DeleteTableRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DeleteTableRequest) ProtoMessage() {}

func (x *DeleteTableRequest) ProtoReflect() protoreflect.Message {
	mi := &file_deephaven_proto_inputtable_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use DeleteTableRequest.ProtoReflect.Descriptor instead.
func (*DeleteTableRequest) Descriptor() ([]byte, []int) {
	return file_deephaven_proto_inputtable_proto_rawDescGZIP(), []int{2}
}

func (x *DeleteTableRequest) GetInputTable() *ticket.Ticket {
	if x != nil {
		return x.InputTable
	}
	return nil
}

func (x *DeleteTableRequest) GetTableToRemove() *ticket.Ticket {
	if x != nil {
		return x.TableToRemove
	}
	return nil
}

type DeleteTableResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *DeleteTableResponse) Reset() {
	*x = DeleteTableResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_deephaven_proto_inputtable_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *DeleteTableResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DeleteTableResponse) ProtoMessage() {}

func (x *DeleteTableResponse) ProtoReflect() protoreflect.Message {
	mi := &file_deephaven_proto_inputtable_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use DeleteTableResponse.ProtoReflect.Descriptor instead.
func (*DeleteTableResponse) Descriptor() ([]byte, []int) {
	return file_deephaven_proto_inputtable_proto_rawDescGZIP(), []int{3}
}

var File_deephaven_proto_inputtable_proto protoreflect.FileDescriptor

var file_deephaven_proto_inputtable_proto_rawDesc = []byte{
	0x0a, 0x20, 0x64, 0x65, 0x65, 0x70, 0x68, 0x61, 0x76, 0x65, 0x6e, 0x2f, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x2f, 0x69, 0x6e, 0x70, 0x75, 0x74, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x12, 0x21, 0x69, 0x6f, 0x2e, 0x64, 0x65, 0x65, 0x70, 0x68, 0x61, 0x76, 0x65, 0x6e,
	0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x62, 0x61, 0x63, 0x6b, 0x70, 0x6c, 0x61, 0x6e, 0x65,
	0x2e, 0x67, 0x72, 0x70, 0x63, 0x1a, 0x20, 0x64, 0x65, 0x65, 0x70, 0x68, 0x61, 0x76, 0x65, 0x6e,
	0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x65, 0x78, 0x74, 0x65, 0x6e, 0x73, 0x69, 0x6f, 0x6e,
	0x73, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x1c, 0x64, 0x65, 0x65, 0x70, 0x68, 0x61, 0x76,
	0x65, 0x6e, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x74, 0x69, 0x63, 0x6b, 0x65, 0x74, 0x2e,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0xaa, 0x01, 0x0a, 0x0f, 0x41, 0x64, 0x64, 0x54, 0x61, 0x62,
	0x6c, 0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x4a, 0x0a, 0x0b, 0x69, 0x6e, 0x70,
	0x75, 0x74, 0x5f, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x29,
	0x2e, 0x69, 0x6f, 0x2e, 0x64, 0x65, 0x65, 0x70, 0x68, 0x61, 0x76, 0x65, 0x6e, 0x2e, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x2e, 0x62, 0x61, 0x63, 0x6b, 0x70, 0x6c, 0x61, 0x6e, 0x65, 0x2e, 0x67, 0x72,
	0x70, 0x63, 0x2e, 0x54, 0x69, 0x63, 0x6b, 0x65, 0x74, 0x52, 0x0a, 0x69, 0x6e, 0x70, 0x75, 0x74,
	0x54, 0x61, 0x62, 0x6c, 0x65, 0x12, 0x4b, 0x0a, 0x0c, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x5f, 0x74,
	0x6f, 0x5f, 0x61, 0x64, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x29, 0x2e, 0x69, 0x6f,
	0x2e, 0x64, 0x65, 0x65, 0x70, 0x68, 0x61, 0x76, 0x65, 0x6e, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x2e, 0x62, 0x61, 0x63, 0x6b, 0x70, 0x6c, 0x61, 0x6e, 0x65, 0x2e, 0x67, 0x72, 0x70, 0x63, 0x2e,
	0x54, 0x69, 0x63, 0x6b, 0x65, 0x74, 0x52, 0x0a, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x54, 0x6f, 0x41,
	0x64, 0x64, 0x22, 0x12, 0x0a, 0x10, 0x41, 0x64, 0x64, 0x54, 0x61, 0x62, 0x6c, 0x65, 0x52, 0x65,
	0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0xb3, 0x01, 0x0a, 0x12, 0x44, 0x65, 0x6c, 0x65, 0x74,
	0x65, 0x54, 0x61, 0x62, 0x6c, 0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x4a, 0x0a,
	0x0b, 0x69, 0x6e, 0x70, 0x75, 0x74, 0x5f, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x18, 0x01, 0x20, 0x01,
	0x28, 0x0b, 0x32, 0x29, 0x2e, 0x69, 0x6f, 0x2e, 0x64, 0x65, 0x65, 0x70, 0x68, 0x61, 0x76, 0x65,
	0x6e, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x62, 0x61, 0x63, 0x6b, 0x70, 0x6c, 0x61, 0x6e,
	0x65, 0x2e, 0x67, 0x72, 0x70, 0x63, 0x2e, 0x54, 0x69, 0x63, 0x6b, 0x65, 0x74, 0x52, 0x0a, 0x69,
	0x6e, 0x70, 0x75, 0x74, 0x54, 0x61, 0x62, 0x6c, 0x65, 0x12, 0x51, 0x0a, 0x0f, 0x74, 0x61, 0x62,
	0x6c, 0x65, 0x5f, 0x74, 0x6f, 0x5f, 0x72, 0x65, 0x6d, 0x6f, 0x76, 0x65, 0x18, 0x02, 0x20, 0x01,
	0x28, 0x0b, 0x32, 0x29, 0x2e, 0x69, 0x6f, 0x2e, 0x64, 0x65, 0x65, 0x70, 0x68, 0x61, 0x76, 0x65,
	0x6e, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x62, 0x61, 0x63, 0x6b, 0x70, 0x6c, 0x61, 0x6e,
	0x65, 0x2e, 0x67, 0x72, 0x70, 0x63, 0x2e, 0x54, 0x69, 0x63, 0x6b, 0x65, 0x74, 0x52, 0x0d, 0x74,
	0x61, 0x62, 0x6c, 0x65, 0x54, 0x6f, 0x52, 0x65, 0x6d, 0x6f, 0x76, 0x65, 0x22, 0x15, 0x0a, 0x13,
	0x44, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x54, 0x61, 0x62, 0x6c, 0x65, 0x52, 0x65, 0x73, 0x70, 0x6f,
	0x6e, 0x73, 0x65, 0x32, 0xb2, 0x02, 0x0a, 0x11, 0x49, 0x6e, 0x70, 0x75, 0x74, 0x54, 0x61, 0x62,
	0x6c, 0x65, 0x53, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x12, 0x87, 0x01, 0x0a, 0x14, 0x41, 0x64,
	0x64, 0x54, 0x61, 0x62, 0x6c, 0x65, 0x54, 0x6f, 0x49, 0x6e, 0x70, 0x75, 0x74, 0x54, 0x61, 0x62,
	0x6c, 0x65, 0x12, 0x32, 0x2e, 0x69, 0x6f, 0x2e, 0x64, 0x65, 0x65, 0x70, 0x68, 0x61, 0x76, 0x65,
	0x6e, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x62, 0x61, 0x63, 0x6b, 0x70, 0x6c, 0x61, 0x6e,
	0x65, 0x2e, 0x67, 0x72, 0x70, 0x63, 0x2e, 0x41, 0x64, 0x64, 0x54, 0x61, 0x62, 0x6c, 0x65, 0x52,
	0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x33, 0x2e, 0x69, 0x6f, 0x2e, 0x64, 0x65, 0x65, 0x70,
	0x68, 0x61, 0x76, 0x65, 0x6e, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x62, 0x61, 0x63, 0x6b,
	0x70, 0x6c, 0x61, 0x6e, 0x65, 0x2e, 0x67, 0x72, 0x70, 0x63, 0x2e, 0x41, 0x64, 0x64, 0x54, 0x61,
	0x62, 0x6c, 0x65, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x06, 0xba, 0xb5, 0x18,
	0x02, 0x08, 0x01, 0x12, 0x92, 0x01, 0x0a, 0x19, 0x44, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x54, 0x61,
	0x62, 0x6c, 0x65, 0x46, 0x72, 0x6f, 0x6d, 0x49, 0x6e, 0x70, 0x75, 0x74, 0x54, 0x61, 0x62, 0x6c,
	0x65, 0x12, 0x35, 0x2e, 0x69, 0x6f, 0x2e, 0x64, 0x65, 0x65, 0x70, 0x68, 0x61, 0x76, 0x65, 0x6e,
	0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x62, 0x61, 0x63, 0x6b, 0x70, 0x6c, 0x61, 0x6e, 0x65,
	0x2e, 0x67, 0x72, 0x70, 0x63, 0x2e, 0x44, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x54, 0x61, 0x62, 0x6c,
	0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x36, 0x2e, 0x69, 0x6f, 0x2e, 0x64, 0x65,
	0x65, 0x70, 0x68, 0x61, 0x76, 0x65, 0x6e, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x62, 0x61,
	0x63, 0x6b, 0x70, 0x6c, 0x61, 0x6e, 0x65, 0x2e, 0x67, 0x72, 0x70, 0x63, 0x2e, 0x44, 0x65, 0x6c,
	0x65, 0x74, 0x65, 0x54, 0x61, 0x62, 0x6c, 0x65, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65,
	0x22, 0x06, 0xba, 0xb5, 0x18, 0x02, 0x08, 0x01, 0x42, 0x46, 0x48, 0x01, 0x50, 0x01, 0x5a, 0x40,
	0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x64, 0x65, 0x65, 0x70, 0x68,
	0x61, 0x76, 0x65, 0x6e, 0x2f, 0x64, 0x65, 0x65, 0x70, 0x68, 0x61, 0x76, 0x65, 0x6e, 0x2d, 0x63,
	0x6f, 0x72, 0x65, 0x2f, 0x67, 0x6f, 0x2f, 0x69, 0x6e, 0x74, 0x65, 0x72, 0x6e, 0x61, 0x6c, 0x2f,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x69, 0x6e, 0x70, 0x75, 0x74, 0x74, 0x61, 0x62, 0x6c, 0x65,
	0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_deephaven_proto_inputtable_proto_rawDescOnce sync.Once
	file_deephaven_proto_inputtable_proto_rawDescData = file_deephaven_proto_inputtable_proto_rawDesc
)

func file_deephaven_proto_inputtable_proto_rawDescGZIP() []byte {
	file_deephaven_proto_inputtable_proto_rawDescOnce.Do(func() {
		file_deephaven_proto_inputtable_proto_rawDescData = protoimpl.X.CompressGZIP(file_deephaven_proto_inputtable_proto_rawDescData)
	})
	return file_deephaven_proto_inputtable_proto_rawDescData
}

var file_deephaven_proto_inputtable_proto_msgTypes = make([]protoimpl.MessageInfo, 4)
var file_deephaven_proto_inputtable_proto_goTypes = []interface{}{
	(*AddTableRequest)(nil),     // 0: io.deephaven.proto.backplane.grpc.AddTableRequest
	(*AddTableResponse)(nil),    // 1: io.deephaven.proto.backplane.grpc.AddTableResponse
	(*DeleteTableRequest)(nil),  // 2: io.deephaven.proto.backplane.grpc.DeleteTableRequest
	(*DeleteTableResponse)(nil), // 3: io.deephaven.proto.backplane.grpc.DeleteTableResponse
	(*ticket.Ticket)(nil),       // 4: io.deephaven.proto.backplane.grpc.Ticket
}
var file_deephaven_proto_inputtable_proto_depIdxs = []int32{
	4, // 0: io.deephaven.proto.backplane.grpc.AddTableRequest.input_table:type_name -> io.deephaven.proto.backplane.grpc.Ticket
	4, // 1: io.deephaven.proto.backplane.grpc.AddTableRequest.table_to_add:type_name -> io.deephaven.proto.backplane.grpc.Ticket
	4, // 2: io.deephaven.proto.backplane.grpc.DeleteTableRequest.input_table:type_name -> io.deephaven.proto.backplane.grpc.Ticket
	4, // 3: io.deephaven.proto.backplane.grpc.DeleteTableRequest.table_to_remove:type_name -> io.deephaven.proto.backplane.grpc.Ticket
	0, // 4: io.deephaven.proto.backplane.grpc.InputTableService.AddTableToInputTable:input_type -> io.deephaven.proto.backplane.grpc.AddTableRequest
	2, // 5: io.deephaven.proto.backplane.grpc.InputTableService.DeleteTableFromInputTable:input_type -> io.deephaven.proto.backplane.grpc.DeleteTableRequest
	1, // 6: io.deephaven.proto.backplane.grpc.InputTableService.AddTableToInputTable:output_type -> io.deephaven.proto.backplane.grpc.AddTableResponse
	3, // 7: io.deephaven.proto.backplane.grpc.InputTableService.DeleteTableFromInputTable:output_type -> io.deephaven.proto.backplane.grpc.DeleteTableResponse
	6, // [6:8] is the sub-list for method output_type
	4, // [4:6] is the sub-list for method input_type
	4, // [4:4] is the sub-list for extension type_name
	4, // [4:4] is the sub-list for extension extendee
	0, // [0:4] is the sub-list for field type_name
}

func init() { file_deephaven_proto_inputtable_proto_init() }
func file_deephaven_proto_inputtable_proto_init() {
	if File_deephaven_proto_inputtable_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_deephaven_proto_inputtable_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*AddTableRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_deephaven_proto_inputtable_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*AddTableResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_deephaven_proto_inputtable_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*DeleteTableRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_deephaven_proto_inputtable_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*DeleteTableResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_deephaven_proto_inputtable_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   4,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_deephaven_proto_inputtable_proto_goTypes,
		DependencyIndexes: file_deephaven_proto_inputtable_proto_depIdxs,
		MessageInfos:      file_deephaven_proto_inputtable_proto_msgTypes,
	}.Build()
	File_deephaven_proto_inputtable_proto = out.File
	file_deephaven_proto_inputtable_proto_rawDesc = nil
	file_deephaven_proto_inputtable_proto_goTypes = nil
	file_deephaven_proto_inputtable_proto_depIdxs = nil
}
