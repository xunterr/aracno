// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.35.1
// 	protoc        v5.28.3
// source: msg.proto

package proto

import (
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

type Error struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *Error) Reset() {
	*x = Error{}
	mi := &file_msg_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Error) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Error) ProtoMessage() {}

func (x *Error) ProtoReflect() protoreflect.Message {
	mi := &file_msg_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Error.ProtoReflect.Descriptor instead.
func (*Error) Descriptor() ([]byte, []int) {
	return file_msg_proto_rawDescGZIP(), []int{0}
}

type Key struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Key []byte `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
}

func (x *Key) Reset() {
	*x = Key{}
	mi := &file_msg_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Key) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Key) ProtoMessage() {}

func (x *Key) ProtoReflect() protoreflect.Message {
	mi := &file_msg_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Key.ProtoReflect.Descriptor instead.
func (*Key) Descriptor() ([]byte, []int) {
	return file_msg_proto_rawDescGZIP(), []int{1}
}

func (x *Key) GetKey() []byte {
	if x != nil {
		return x.Key
	}
	return nil
}

type Node struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Id   []byte `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	Addr string `protobuf:"bytes,2,opt,name=addr,proto3" json:"addr,omitempty"`
}

func (x *Node) Reset() {
	*x = Node{}
	mi := &file_msg_proto_msgTypes[2]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Node) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Node) ProtoMessage() {}

func (x *Node) ProtoReflect() protoreflect.Message {
	mi := &file_msg_proto_msgTypes[2]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Node.ProtoReflect.Descriptor instead.
func (*Node) Descriptor() ([]byte, []int) {
	return file_msg_proto_rawDescGZIP(), []int{2}
}

func (x *Node) GetId() []byte {
	if x != nil {
		return x.Id
	}
	return nil
}

func (x *Node) GetAddr() string {
	if x != nil {
		return x.Addr
	}
	return ""
}

type Finger struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	I    int64 `protobuf:"varint,1,opt,name=i,proto3" json:"i,omitempty"`
	Node *Node `protobuf:"bytes,2,opt,name=node,proto3" json:"node,omitempty"`
}

func (x *Finger) Reset() {
	*x = Finger{}
	mi := &file_msg_proto_msgTypes[3]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Finger) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Finger) ProtoMessage() {}

func (x *Finger) ProtoReflect() protoreflect.Message {
	mi := &file_msg_proto_msgTypes[3]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Finger.ProtoReflect.Descriptor instead.
func (*Finger) Descriptor() ([]byte, []int) {
	return file_msg_proto_rawDescGZIP(), []int{3}
}

func (x *Finger) GetI() int64 {
	if x != nil {
		return x.I
	}
	return 0
}

func (x *Finger) GetNode() *Node {
	if x != nil {
		return x.Node
	}
	return nil
}

type SuccList struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Node []*Node `protobuf:"bytes,1,rep,name=node,proto3" json:"node,omitempty"`
}

func (x *SuccList) Reset() {
	*x = SuccList{}
	mi := &file_msg_proto_msgTypes[4]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *SuccList) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SuccList) ProtoMessage() {}

func (x *SuccList) ProtoReflect() protoreflect.Message {
	mi := &file_msg_proto_msgTypes[4]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SuccList.ProtoReflect.Descriptor instead.
func (*SuccList) Descriptor() ([]byte, []int) {
	return file_msg_proto_rawDescGZIP(), []int{4}
}

func (x *SuccList) GetNode() []*Node {
	if x != nil {
		return x.Node
	}
	return nil
}

type URL struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Url string `protobuf:"bytes,1,opt,name=url,proto3" json:"url,omitempty"`
}

func (x *URL) Reset() {
	*x = URL{}
	mi := &file_msg_proto_msgTypes[5]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *URL) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*URL) ProtoMessage() {}

func (x *URL) ProtoReflect() protoreflect.Message {
	mi := &file_msg_proto_msgTypes[5]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use URL.ProtoReflect.Descriptor instead.
func (*URL) Descriptor() ([]byte, []int) {
	return file_msg_proto_rawDescGZIP(), []int{5}
}

func (x *URL) GetUrl() string {
	if x != nil {
		return x.Url
	}
	return ""
}

type UrlBatch struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Url []string `protobuf:"bytes,1,rep,name=url,proto3" json:"url,omitempty"`
}

func (x *UrlBatch) Reset() {
	*x = UrlBatch{}
	mi := &file_msg_proto_msgTypes[6]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *UrlBatch) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*UrlBatch) ProtoMessage() {}

func (x *UrlBatch) ProtoReflect() protoreflect.Message {
	mi := &file_msg_proto_msgTypes[6]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use UrlBatch.ProtoReflect.Descriptor instead.
func (*UrlBatch) Descriptor() ([]byte, []int) {
	return file_msg_proto_rawDescGZIP(), []int{6}
}

func (x *UrlBatch) GetUrl() []string {
	if x != nil {
		return x.Url
	}
	return nil
}

type DispatcherRoute struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Key string `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	Url string `protobuf:"bytes,2,opt,name=url,proto3" json:"url,omitempty"`
}

func (x *DispatcherRoute) Reset() {
	*x = DispatcherRoute{}
	mi := &file_msg_proto_msgTypes[7]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *DispatcherRoute) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DispatcherRoute) ProtoMessage() {}

func (x *DispatcherRoute) ProtoReflect() protoreflect.Message {
	mi := &file_msg_proto_msgTypes[7]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use DispatcherRoute.ProtoReflect.Descriptor instead.
func (*DispatcherRoute) Descriptor() ([]byte, []int) {
	return file_msg_proto_rawDescGZIP(), []int{7}
}

func (x *DispatcherRoute) GetKey() string {
	if x != nil {
		return x.Key
	}
	return ""
}

func (x *DispatcherRoute) GetUrl() string {
	if x != nil {
		return x.Url
	}
	return ""
}

type KeyLockNotification struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Key   string `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	Bloom []byte `protobuf:"bytes,2,opt,name=bloom,proto3" json:"bloom,omitempty"`
}

func (x *KeyLockNotification) Reset() {
	*x = KeyLockNotification{}
	mi := &file_msg_proto_msgTypes[8]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *KeyLockNotification) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*KeyLockNotification) ProtoMessage() {}

func (x *KeyLockNotification) ProtoReflect() protoreflect.Message {
	mi := &file_msg_proto_msgTypes[8]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use KeyLockNotification.ProtoReflect.Descriptor instead.
func (*KeyLockNotification) Descriptor() ([]byte, []int) {
	return file_msg_proto_rawDescGZIP(), []int{8}
}

func (x *KeyLockNotification) GetKey() string {
	if x != nil {
		return x.Key
	}
	return ""
}

func (x *KeyLockNotification) GetBloom() []byte {
	if x != nil {
		return x.Bloom
	}
	return nil
}

var File_msg_proto protoreflect.FileDescriptor

var file_msg_proto_rawDesc = []byte{
	0x0a, 0x09, 0x6d, 0x73, 0x67, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x07, 0x70, 0x61, 0x63,
	0x6b, 0x61, 0x67, 0x65, 0x22, 0x07, 0x0a, 0x05, 0x45, 0x72, 0x72, 0x6f, 0x72, 0x22, 0x17, 0x0a,
	0x03, 0x4b, 0x65, 0x79, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x0c, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x22, 0x2a, 0x0a, 0x04, 0x4e, 0x6f, 0x64, 0x65, 0x12, 0x0e,
	0x0a, 0x02, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x02, 0x69, 0x64, 0x12, 0x12,
	0x0a, 0x04, 0x61, 0x64, 0x64, 0x72, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x61, 0x64,
	0x64, 0x72, 0x22, 0x39, 0x0a, 0x06, 0x46, 0x69, 0x6e, 0x67, 0x65, 0x72, 0x12, 0x0c, 0x0a, 0x01,
	0x69, 0x18, 0x01, 0x20, 0x01, 0x28, 0x03, 0x52, 0x01, 0x69, 0x12, 0x21, 0x0a, 0x04, 0x6e, 0x6f,
	0x64, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x0d, 0x2e, 0x70, 0x61, 0x63, 0x6b, 0x61,
	0x67, 0x65, 0x2e, 0x4e, 0x6f, 0x64, 0x65, 0x52, 0x04, 0x6e, 0x6f, 0x64, 0x65, 0x22, 0x2d, 0x0a,
	0x08, 0x53, 0x75, 0x63, 0x63, 0x4c, 0x69, 0x73, 0x74, 0x12, 0x21, 0x0a, 0x04, 0x6e, 0x6f, 0x64,
	0x65, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x0d, 0x2e, 0x70, 0x61, 0x63, 0x6b, 0x61, 0x67,
	0x65, 0x2e, 0x4e, 0x6f, 0x64, 0x65, 0x52, 0x04, 0x6e, 0x6f, 0x64, 0x65, 0x22, 0x17, 0x0a, 0x03,
	0x55, 0x52, 0x4c, 0x12, 0x10, 0x0a, 0x03, 0x75, 0x72, 0x6c, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09,
	0x52, 0x03, 0x75, 0x72, 0x6c, 0x22, 0x1c, 0x0a, 0x08, 0x55, 0x72, 0x6c, 0x42, 0x61, 0x74, 0x63,
	0x68, 0x12, 0x10, 0x0a, 0x03, 0x75, 0x72, 0x6c, 0x18, 0x01, 0x20, 0x03, 0x28, 0x09, 0x52, 0x03,
	0x75, 0x72, 0x6c, 0x22, 0x35, 0x0a, 0x0f, 0x44, 0x69, 0x73, 0x70, 0x61, 0x74, 0x63, 0x68, 0x65,
	0x72, 0x52, 0x6f, 0x75, 0x74, 0x65, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x09, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x10, 0x0a, 0x03, 0x75, 0x72, 0x6c, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x75, 0x72, 0x6c, 0x22, 0x3d, 0x0a, 0x13, 0x4b, 0x65,
	0x79, 0x4c, 0x6f, 0x63, 0x6b, 0x4e, 0x6f, 0x74, 0x69, 0x66, 0x69, 0x63, 0x61, 0x74, 0x69, 0x6f,
	0x6e, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03,
	0x6b, 0x65, 0x79, 0x12, 0x14, 0x0a, 0x05, 0x62, 0x6c, 0x6f, 0x6f, 0x6d, 0x18, 0x02, 0x20, 0x01,
	0x28, 0x0c, 0x52, 0x05, 0x62, 0x6c, 0x6f, 0x6f, 0x6d, 0x42, 0x08, 0x5a, 0x06, 0x2f, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_msg_proto_rawDescOnce sync.Once
	file_msg_proto_rawDescData = file_msg_proto_rawDesc
)

func file_msg_proto_rawDescGZIP() []byte {
	file_msg_proto_rawDescOnce.Do(func() {
		file_msg_proto_rawDescData = protoimpl.X.CompressGZIP(file_msg_proto_rawDescData)
	})
	return file_msg_proto_rawDescData
}

var file_msg_proto_msgTypes = make([]protoimpl.MessageInfo, 9)
var file_msg_proto_goTypes = []any{
	(*Error)(nil),               // 0: package.Error
	(*Key)(nil),                 // 1: package.Key
	(*Node)(nil),                // 2: package.Node
	(*Finger)(nil),              // 3: package.Finger
	(*SuccList)(nil),            // 4: package.SuccList
	(*URL)(nil),                 // 5: package.URL
	(*UrlBatch)(nil),            // 6: package.UrlBatch
	(*DispatcherRoute)(nil),     // 7: package.DispatcherRoute
	(*KeyLockNotification)(nil), // 8: package.KeyLockNotification
}
var file_msg_proto_depIdxs = []int32{
	2, // 0: package.Finger.node:type_name -> package.Node
	2, // 1: package.SuccList.node:type_name -> package.Node
	2, // [2:2] is the sub-list for method output_type
	2, // [2:2] is the sub-list for method input_type
	2, // [2:2] is the sub-list for extension type_name
	2, // [2:2] is the sub-list for extension extendee
	0, // [0:2] is the sub-list for field type_name
}

func init() { file_msg_proto_init() }
func file_msg_proto_init() {
	if File_msg_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_msg_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   9,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_msg_proto_goTypes,
		DependencyIndexes: file_msg_proto_depIdxs,
		MessageInfos:      file_msg_proto_msgTypes,
	}.Build()
	File_msg_proto = out.File
	file_msg_proto_rawDesc = nil
	file_msg_proto_goTypes = nil
	file_msg_proto_depIdxs = nil
}
