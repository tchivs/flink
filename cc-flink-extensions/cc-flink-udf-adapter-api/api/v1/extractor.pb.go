//
// Copyright 2024 Confluent Inc.

// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.32.0
// 	protoc        v3.21.5
// source: cc-flink-extensions/cc-flink-udf-adapter-api/api/v1/extractor.proto

package v1

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

type ErrorCode int32

const (
	ErrorCode_UNKNOWN                      ErrorCode = 0
	ErrorCode_GENERAL_FAILED_EXTRACTION    ErrorCode = 1
	ErrorCode_USER_CODE_NOT_FOUND          ErrorCode = 2
	ErrorCode_NOT_SCALAR_FUNCTION          ErrorCode = 3
	ErrorCode_NO_PUBLIC_NO_ARG_CONSTRUCTOR ErrorCode = 4
	ErrorCode_TYPE_EXTRACTION_ERROR        ErrorCode = 5
	ErrorCode_BAD_SIGNATURE_TYPE           ErrorCode = 6
)

// Enum value maps for ErrorCode.
var (
	ErrorCode_name = map[int32]string{
		0: "UNKNOWN",
		1: "GENERAL_FAILED_EXTRACTION",
		2: "USER_CODE_NOT_FOUND",
		3: "NOT_SCALAR_FUNCTION",
		4: "NO_PUBLIC_NO_ARG_CONSTRUCTOR",
		5: "TYPE_EXTRACTION_ERROR",
		6: "BAD_SIGNATURE_TYPE",
	}
	ErrorCode_value = map[string]int32{
		"UNKNOWN":                      0,
		"GENERAL_FAILED_EXTRACTION":    1,
		"USER_CODE_NOT_FOUND":          2,
		"NOT_SCALAR_FUNCTION":          3,
		"NO_PUBLIC_NO_ARG_CONSTRUCTOR": 4,
		"TYPE_EXTRACTION_ERROR":        5,
		"BAD_SIGNATURE_TYPE":           6,
	}
)

func (x ErrorCode) Enum() *ErrorCode {
	p := new(ErrorCode)
	*p = x
	return p
}

func (x ErrorCode) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (ErrorCode) Descriptor() protoreflect.EnumDescriptor {
	return file_cc_flink_extensions_cc_flink_udf_adapter_api_api_v1_extractor_proto_enumTypes[0].Descriptor()
}

func (ErrorCode) Type() protoreflect.EnumType {
	return &file_cc_flink_extensions_cc_flink_udf_adapter_api_api_v1_extractor_proto_enumTypes[0]
}

func (x ErrorCode) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use ErrorCode.Descriptor instead.
func (ErrorCode) EnumDescriptor() ([]byte, []int) {
	return file_cc_flink_extensions_cc_flink_udf_adapter_api_api_v1_extractor_proto_rawDescGZIP(), []int{0}
}

type Error struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Code         ErrorCode `protobuf:"varint,1,opt,name=code,proto3,enum=flink.udf.extractor.v1.ErrorCode" json:"code,omitempty"`
	ErrorMessage string    `protobuf:"bytes,2,opt,name=error_message,json=errorMessage,proto3" json:"error_message,omitempty"`
}

func (x *Error) Reset() {
	*x = Error{}
	if protoimpl.UnsafeEnabled {
		mi := &file_cc_flink_extensions_cc_flink_udf_adapter_api_api_v1_extractor_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Error) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Error) ProtoMessage() {}

func (x *Error) ProtoReflect() protoreflect.Message {
	mi := &file_cc_flink_extensions_cc_flink_udf_adapter_api_api_v1_extractor_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
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
	return file_cc_flink_extensions_cc_flink_udf_adapter_api_api_v1_extractor_proto_rawDescGZIP(), []int{0}
}

func (x *Error) GetCode() ErrorCode {
	if x != nil {
		return x.Code
	}
	return ErrorCode_UNKNOWN
}

func (x *Error) GetErrorMessage() string {
	if x != nil {
		return x.ErrorMessage
	}
	return ""
}

type ExtractionRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	PluginId        string `protobuf:"bytes,1,opt,name=plugin_id,json=pluginId,proto3" json:"plugin_id,omitempty"`
	PluginVersionId string `protobuf:"bytes,2,opt,name=plugin_version_id,json=pluginVersionId,proto3" json:"plugin_version_id,omitempty"`
	ClassName       string `protobuf:"bytes,3,opt,name=class_name,json=className,proto3" json:"class_name,omitempty"`
}

func (x *ExtractionRequest) Reset() {
	*x = ExtractionRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_cc_flink_extensions_cc_flink_udf_adapter_api_api_v1_extractor_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ExtractionRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ExtractionRequest) ProtoMessage() {}

func (x *ExtractionRequest) ProtoReflect() protoreflect.Message {
	mi := &file_cc_flink_extensions_cc_flink_udf_adapter_api_api_v1_extractor_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ExtractionRequest.ProtoReflect.Descriptor instead.
func (*ExtractionRequest) Descriptor() ([]byte, []int) {
	return file_cc_flink_extensions_cc_flink_udf_adapter_api_api_v1_extractor_proto_rawDescGZIP(), []int{1}
}

func (x *ExtractionRequest) GetPluginId() string {
	if x != nil {
		return x.PluginId
	}
	return ""
}

func (x *ExtractionRequest) GetPluginVersionId() string {
	if x != nil {
		return x.PluginVersionId
	}
	return ""
}

func (x *ExtractionRequest) GetClassName() string {
	if x != nil {
		return x.ClassName
	}
	return ""
}

type Signature struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ArgumentTypes []string `protobuf:"bytes,1,rep,name=argumentTypes,proto3" json:"argumentTypes,omitempty"`
	ReturnType    string   `protobuf:"bytes,2,opt,name=returnType,proto3" json:"returnType,omitempty"`
}

func (x *Signature) Reset() {
	*x = Signature{}
	if protoimpl.UnsafeEnabled {
		mi := &file_cc_flink_extensions_cc_flink_udf_adapter_api_api_v1_extractor_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Signature) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Signature) ProtoMessage() {}

func (x *Signature) ProtoReflect() protoreflect.Message {
	mi := &file_cc_flink_extensions_cc_flink_udf_adapter_api_api_v1_extractor_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Signature.ProtoReflect.Descriptor instead.
func (*Signature) Descriptor() ([]byte, []int) {
	return file_cc_flink_extensions_cc_flink_udf_adapter_api_api_v1_extractor_proto_rawDescGZIP(), []int{2}
}

func (x *Signature) GetArgumentTypes() []string {
	if x != nil {
		return x.ArgumentTypes
	}
	return nil
}

func (x *Signature) GetReturnType() string {
	if x != nil {
		return x.ReturnType
	}
	return ""
}

type ExtractionResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Error      *Error       `protobuf:"bytes,1,opt,name=error,proto3" json:"error,omitempty"`
	Signatures []*Signature `protobuf:"bytes,2,rep,name=signatures,proto3" json:"signatures,omitempty"`
}

func (x *ExtractionResponse) Reset() {
	*x = ExtractionResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_cc_flink_extensions_cc_flink_udf_adapter_api_api_v1_extractor_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ExtractionResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ExtractionResponse) ProtoMessage() {}

func (x *ExtractionResponse) ProtoReflect() protoreflect.Message {
	mi := &file_cc_flink_extensions_cc_flink_udf_adapter_api_api_v1_extractor_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ExtractionResponse.ProtoReflect.Descriptor instead.
func (*ExtractionResponse) Descriptor() ([]byte, []int) {
	return file_cc_flink_extensions_cc_flink_udf_adapter_api_api_v1_extractor_proto_rawDescGZIP(), []int{3}
}

func (x *ExtractionResponse) GetError() *Error {
	if x != nil {
		return x.Error
	}
	return nil
}

func (x *ExtractionResponse) GetSignatures() []*Signature {
	if x != nil {
		return x.Signatures
	}
	return nil
}

var File_cc_flink_extensions_cc_flink_udf_adapter_api_api_v1_extractor_proto protoreflect.FileDescriptor

var file_cc_flink_extensions_cc_flink_udf_adapter_api_api_v1_extractor_proto_rawDesc = []byte{
	0x0a, 0x43, 0x63, 0x63, 0x2d, 0x66, 0x6c, 0x69, 0x6e, 0x6b, 0x2d, 0x65, 0x78, 0x74, 0x65, 0x6e,
	0x73, 0x69, 0x6f, 0x6e, 0x73, 0x2f, 0x63, 0x63, 0x2d, 0x66, 0x6c, 0x69, 0x6e, 0x6b, 0x2d, 0x75,
	0x64, 0x66, 0x2d, 0x61, 0x64, 0x61, 0x70, 0x74, 0x65, 0x72, 0x2d, 0x61, 0x70, 0x69, 0x2f, 0x61,
	0x70, 0x69, 0x2f, 0x76, 0x31, 0x2f, 0x65, 0x78, 0x74, 0x72, 0x61, 0x63, 0x74, 0x6f, 0x72, 0x2e,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x16, 0x66, 0x6c, 0x69, 0x6e, 0x6b, 0x2e, 0x75, 0x64, 0x66,
	0x2e, 0x65, 0x78, 0x74, 0x72, 0x61, 0x63, 0x74, 0x6f, 0x72, 0x2e, 0x76, 0x31, 0x22, 0x63, 0x0a,
	0x05, 0x45, 0x72, 0x72, 0x6f, 0x72, 0x12, 0x35, 0x0a, 0x04, 0x63, 0x6f, 0x64, 0x65, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x0e, 0x32, 0x21, 0x2e, 0x66, 0x6c, 0x69, 0x6e, 0x6b, 0x2e, 0x75, 0x64, 0x66,
	0x2e, 0x65, 0x78, 0x74, 0x72, 0x61, 0x63, 0x74, 0x6f, 0x72, 0x2e, 0x76, 0x31, 0x2e, 0x45, 0x72,
	0x72, 0x6f, 0x72, 0x43, 0x6f, 0x64, 0x65, 0x52, 0x04, 0x63, 0x6f, 0x64, 0x65, 0x12, 0x23, 0x0a,
	0x0d, 0x65, 0x72, 0x72, 0x6f, 0x72, 0x5f, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x18, 0x02,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x0c, 0x65, 0x72, 0x72, 0x6f, 0x72, 0x4d, 0x65, 0x73, 0x73, 0x61,
	0x67, 0x65, 0x22, 0x7b, 0x0a, 0x11, 0x45, 0x78, 0x74, 0x72, 0x61, 0x63, 0x74, 0x69, 0x6f, 0x6e,
	0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x1b, 0x0a, 0x09, 0x70, 0x6c, 0x75, 0x67, 0x69,
	0x6e, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x70, 0x6c, 0x75, 0x67,
	0x69, 0x6e, 0x49, 0x64, 0x12, 0x2a, 0x0a, 0x11, 0x70, 0x6c, 0x75, 0x67, 0x69, 0x6e, 0x5f, 0x76,
	0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x5f, 0x69, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x0f, 0x70, 0x6c, 0x75, 0x67, 0x69, 0x6e, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x49, 0x64,
	0x12, 0x1d, 0x0a, 0x0a, 0x63, 0x6c, 0x61, 0x73, 0x73, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x03,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x63, 0x6c, 0x61, 0x73, 0x73, 0x4e, 0x61, 0x6d, 0x65, 0x22,
	0x51, 0x0a, 0x09, 0x53, 0x69, 0x67, 0x6e, 0x61, 0x74, 0x75, 0x72, 0x65, 0x12, 0x24, 0x0a, 0x0d,
	0x61, 0x72, 0x67, 0x75, 0x6d, 0x65, 0x6e, 0x74, 0x54, 0x79, 0x70, 0x65, 0x73, 0x18, 0x01, 0x20,
	0x03, 0x28, 0x09, 0x52, 0x0d, 0x61, 0x72, 0x67, 0x75, 0x6d, 0x65, 0x6e, 0x74, 0x54, 0x79, 0x70,
	0x65, 0x73, 0x12, 0x1e, 0x0a, 0x0a, 0x72, 0x65, 0x74, 0x75, 0x72, 0x6e, 0x54, 0x79, 0x70, 0x65,
	0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0a, 0x72, 0x65, 0x74, 0x75, 0x72, 0x6e, 0x54, 0x79,
	0x70, 0x65, 0x22, 0x8c, 0x01, 0x0a, 0x12, 0x45, 0x78, 0x74, 0x72, 0x61, 0x63, 0x74, 0x69, 0x6f,
	0x6e, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x33, 0x0a, 0x05, 0x65, 0x72, 0x72,
	0x6f, 0x72, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1d, 0x2e, 0x66, 0x6c, 0x69, 0x6e, 0x6b,
	0x2e, 0x75, 0x64, 0x66, 0x2e, 0x65, 0x78, 0x74, 0x72, 0x61, 0x63, 0x74, 0x6f, 0x72, 0x2e, 0x76,
	0x31, 0x2e, 0x45, 0x72, 0x72, 0x6f, 0x72, 0x52, 0x05, 0x65, 0x72, 0x72, 0x6f, 0x72, 0x12, 0x41,
	0x0a, 0x0a, 0x73, 0x69, 0x67, 0x6e, 0x61, 0x74, 0x75, 0x72, 0x65, 0x73, 0x18, 0x02, 0x20, 0x03,
	0x28, 0x0b, 0x32, 0x21, 0x2e, 0x66, 0x6c, 0x69, 0x6e, 0x6b, 0x2e, 0x75, 0x64, 0x66, 0x2e, 0x65,
	0x78, 0x74, 0x72, 0x61, 0x63, 0x74, 0x6f, 0x72, 0x2e, 0x76, 0x31, 0x2e, 0x53, 0x69, 0x67, 0x6e,
	0x61, 0x74, 0x75, 0x72, 0x65, 0x52, 0x0a, 0x73, 0x69, 0x67, 0x6e, 0x61, 0x74, 0x75, 0x72, 0x65,
	0x73, 0x2a, 0xbe, 0x01, 0x0a, 0x09, 0x45, 0x72, 0x72, 0x6f, 0x72, 0x43, 0x6f, 0x64, 0x65, 0x12,
	0x0b, 0x0a, 0x07, 0x55, 0x4e, 0x4b, 0x4e, 0x4f, 0x57, 0x4e, 0x10, 0x00, 0x12, 0x1d, 0x0a, 0x19,
	0x47, 0x45, 0x4e, 0x45, 0x52, 0x41, 0x4c, 0x5f, 0x46, 0x41, 0x49, 0x4c, 0x45, 0x44, 0x5f, 0x45,
	0x58, 0x54, 0x52, 0x41, 0x43, 0x54, 0x49, 0x4f, 0x4e, 0x10, 0x01, 0x12, 0x17, 0x0a, 0x13, 0x55,
	0x53, 0x45, 0x52, 0x5f, 0x43, 0x4f, 0x44, 0x45, 0x5f, 0x4e, 0x4f, 0x54, 0x5f, 0x46, 0x4f, 0x55,
	0x4e, 0x44, 0x10, 0x02, 0x12, 0x17, 0x0a, 0x13, 0x4e, 0x4f, 0x54, 0x5f, 0x53, 0x43, 0x41, 0x4c,
	0x41, 0x52, 0x5f, 0x46, 0x55, 0x4e, 0x43, 0x54, 0x49, 0x4f, 0x4e, 0x10, 0x03, 0x12, 0x20, 0x0a,
	0x1c, 0x4e, 0x4f, 0x5f, 0x50, 0x55, 0x42, 0x4c, 0x49, 0x43, 0x5f, 0x4e, 0x4f, 0x5f, 0x41, 0x52,
	0x47, 0x5f, 0x43, 0x4f, 0x4e, 0x53, 0x54, 0x52, 0x55, 0x43, 0x54, 0x4f, 0x52, 0x10, 0x04, 0x12,
	0x19, 0x0a, 0x15, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x45, 0x58, 0x54, 0x52, 0x41, 0x43, 0x54, 0x49,
	0x4f, 0x4e, 0x5f, 0x45, 0x52, 0x52, 0x4f, 0x52, 0x10, 0x05, 0x12, 0x16, 0x0a, 0x12, 0x42, 0x41,
	0x44, 0x5f, 0x53, 0x49, 0x47, 0x4e, 0x41, 0x54, 0x55, 0x52, 0x45, 0x5f, 0x54, 0x59, 0x50, 0x45,
	0x10, 0x06, 0x42, 0x7d, 0x0a, 0x23, 0x69, 0x6f, 0x2e, 0x63, 0x6f, 0x6e, 0x66, 0x6c, 0x75, 0x65,
	0x6e, 0x74, 0x2e, 0x66, 0x6c, 0x69, 0x6e, 0x6b, 0x2e, 0x75, 0x64, 0x66, 0x2e, 0x65, 0x78, 0x74,
	0x72, 0x61, 0x63, 0x74, 0x6f, 0x72, 0x2e, 0x76, 0x31, 0x50, 0x01, 0x5a, 0x54, 0x67, 0x69, 0x74,
	0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x63, 0x6f, 0x6e, 0x66, 0x6c, 0x75, 0x65, 0x6e,
	0x74, 0x69, 0x6e, 0x63, 0x2f, 0x66, 0x6c, 0x69, 0x6e, 0x6b, 0x2f, 0x63, 0x63, 0x2d, 0x66, 0x6c,
	0x69, 0x6e, 0x6b, 0x2d, 0x65, 0x78, 0x74, 0x65, 0x6e, 0x73, 0x69, 0x6f, 0x6e, 0x73, 0x2f, 0x63,
	0x63, 0x2d, 0x66, 0x6c, 0x69, 0x6e, 0x6b, 0x2d, 0x75, 0x64, 0x66, 0x2d, 0x61, 0x64, 0x61, 0x70,
	0x74, 0x65, 0x72, 0x2d, 0x61, 0x70, 0x69, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x76, 0x31, 0x3b, 0x76,
	0x31, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_cc_flink_extensions_cc_flink_udf_adapter_api_api_v1_extractor_proto_rawDescOnce sync.Once
	file_cc_flink_extensions_cc_flink_udf_adapter_api_api_v1_extractor_proto_rawDescData = file_cc_flink_extensions_cc_flink_udf_adapter_api_api_v1_extractor_proto_rawDesc
)

func file_cc_flink_extensions_cc_flink_udf_adapter_api_api_v1_extractor_proto_rawDescGZIP() []byte {
	file_cc_flink_extensions_cc_flink_udf_adapter_api_api_v1_extractor_proto_rawDescOnce.Do(func() {
		file_cc_flink_extensions_cc_flink_udf_adapter_api_api_v1_extractor_proto_rawDescData = protoimpl.X.CompressGZIP(file_cc_flink_extensions_cc_flink_udf_adapter_api_api_v1_extractor_proto_rawDescData)
	})
	return file_cc_flink_extensions_cc_flink_udf_adapter_api_api_v1_extractor_proto_rawDescData
}

var file_cc_flink_extensions_cc_flink_udf_adapter_api_api_v1_extractor_proto_enumTypes = make([]protoimpl.EnumInfo, 1)
var file_cc_flink_extensions_cc_flink_udf_adapter_api_api_v1_extractor_proto_msgTypes = make([]protoimpl.MessageInfo, 4)
var file_cc_flink_extensions_cc_flink_udf_adapter_api_api_v1_extractor_proto_goTypes = []interface{}{
	(ErrorCode)(0),             // 0: flink.udf.extractor.v1.ErrorCode
	(*Error)(nil),              // 1: flink.udf.extractor.v1.Error
	(*ExtractionRequest)(nil),  // 2: flink.udf.extractor.v1.ExtractionRequest
	(*Signature)(nil),          // 3: flink.udf.extractor.v1.Signature
	(*ExtractionResponse)(nil), // 4: flink.udf.extractor.v1.ExtractionResponse
}
var file_cc_flink_extensions_cc_flink_udf_adapter_api_api_v1_extractor_proto_depIdxs = []int32{
	0, // 0: flink.udf.extractor.v1.Error.code:type_name -> flink.udf.extractor.v1.ErrorCode
	1, // 1: flink.udf.extractor.v1.ExtractionResponse.error:type_name -> flink.udf.extractor.v1.Error
	3, // 2: flink.udf.extractor.v1.ExtractionResponse.signatures:type_name -> flink.udf.extractor.v1.Signature
	3, // [3:3] is the sub-list for method output_type
	3, // [3:3] is the sub-list for method input_type
	3, // [3:3] is the sub-list for extension type_name
	3, // [3:3] is the sub-list for extension extendee
	0, // [0:3] is the sub-list for field type_name
}

func init() { file_cc_flink_extensions_cc_flink_udf_adapter_api_api_v1_extractor_proto_init() }
func file_cc_flink_extensions_cc_flink_udf_adapter_api_api_v1_extractor_proto_init() {
	if File_cc_flink_extensions_cc_flink_udf_adapter_api_api_v1_extractor_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_cc_flink_extensions_cc_flink_udf_adapter_api_api_v1_extractor_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Error); i {
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
		file_cc_flink_extensions_cc_flink_udf_adapter_api_api_v1_extractor_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ExtractionRequest); i {
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
		file_cc_flink_extensions_cc_flink_udf_adapter_api_api_v1_extractor_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Signature); i {
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
		file_cc_flink_extensions_cc_flink_udf_adapter_api_api_v1_extractor_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ExtractionResponse); i {
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
			RawDescriptor: file_cc_flink_extensions_cc_flink_udf_adapter_api_api_v1_extractor_proto_rawDesc,
			NumEnums:      1,
			NumMessages:   4,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_cc_flink_extensions_cc_flink_udf_adapter_api_api_v1_extractor_proto_goTypes,
		DependencyIndexes: file_cc_flink_extensions_cc_flink_udf_adapter_api_api_v1_extractor_proto_depIdxs,
		EnumInfos:         file_cc_flink_extensions_cc_flink_udf_adapter_api_api_v1_extractor_proto_enumTypes,
		MessageInfos:      file_cc_flink_extensions_cc_flink_udf_adapter_api_api_v1_extractor_proto_msgTypes,
	}.Build()
	File_cc_flink_extensions_cc_flink_udf_adapter_api_api_v1_extractor_proto = out.File
	file_cc_flink_extensions_cc_flink_udf_adapter_api_api_v1_extractor_proto_rawDesc = nil
	file_cc_flink_extensions_cc_flink_udf_adapter_api_api_v1_extractor_proto_goTypes = nil
	file_cc_flink_extensions_cc_flink_udf_adapter_api_api_v1_extractor_proto_depIdxs = nil
}
