//
//Copyright 2019 The Vitess Authors.
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.

// Service definition for vtgateservice.
// This is the main entry point to Vitess.

// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.36.6
// 	protoc        v3.21.3
// source: vtgateservice.proto

package vtgateservice

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	unsafe "unsafe"
	vtgate "vitess.io/vitess/go/vt/proto/vtgate"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

var File_vtgateservice_proto protoreflect.FileDescriptor

const file_vtgateservice_proto_rawDesc = "" +
	"\n" +
	"\x13vtgateservice.proto\x12\rvtgateservice\x1a\fvtgate.proto2\xde\x04\n" +
	"\x06Vitess\x12<\n" +
	"\aExecute\x12\x16.vtgate.ExecuteRequest\x1a\x17.vtgate.ExecuteResponse\"\x00\x12K\n" +
	"\fExecuteMulti\x12\x1b.vtgate.ExecuteMultiRequest\x1a\x1c.vtgate.ExecuteMultiResponse\"\x00\x12K\n" +
	"\fExecuteBatch\x12\x1b.vtgate.ExecuteBatchRequest\x1a\x1c.vtgate.ExecuteBatchResponse\"\x00\x12P\n" +
	"\rStreamExecute\x12\x1c.vtgate.StreamExecuteRequest\x1a\x1d.vtgate.StreamExecuteResponse\"\x000\x01\x12_\n" +
	"\x12StreamExecuteMulti\x12!.vtgate.StreamExecuteMultiRequest\x1a\".vtgate.StreamExecuteMultiResponse\"\x000\x01\x12>\n" +
	"\aVStream\x12\x16.vtgate.VStreamRequest\x1a\x17.vtgate.VStreamResponse\"\x000\x01\x12<\n" +
	"\aPrepare\x12\x16.vtgate.PrepareRequest\x1a\x17.vtgate.PrepareResponse\"\x00\x12K\n" +
	"\fCloseSession\x12\x1b.vtgate.CloseSessionRequest\x1a\x1c.vtgate.CloseSessionResponse\"\x00BB\n" +
	"\x14io.vitess.proto.grpcZ*vitess.io/vitess/go/vt/proto/vtgateserviceb\x06proto3"

var file_vtgateservice_proto_goTypes = []any{
	(*vtgate.ExecuteRequest)(nil),             // 0: vtgate.ExecuteRequest
	(*vtgate.ExecuteMultiRequest)(nil),        // 1: vtgate.ExecuteMultiRequest
	(*vtgate.ExecuteBatchRequest)(nil),        // 2: vtgate.ExecuteBatchRequest
	(*vtgate.StreamExecuteRequest)(nil),       // 3: vtgate.StreamExecuteRequest
	(*vtgate.StreamExecuteMultiRequest)(nil),  // 4: vtgate.StreamExecuteMultiRequest
	(*vtgate.VStreamRequest)(nil),             // 5: vtgate.VStreamRequest
	(*vtgate.PrepareRequest)(nil),             // 6: vtgate.PrepareRequest
	(*vtgate.CloseSessionRequest)(nil),        // 7: vtgate.CloseSessionRequest
	(*vtgate.ExecuteResponse)(nil),            // 8: vtgate.ExecuteResponse
	(*vtgate.ExecuteMultiResponse)(nil),       // 9: vtgate.ExecuteMultiResponse
	(*vtgate.ExecuteBatchResponse)(nil),       // 10: vtgate.ExecuteBatchResponse
	(*vtgate.StreamExecuteResponse)(nil),      // 11: vtgate.StreamExecuteResponse
	(*vtgate.StreamExecuteMultiResponse)(nil), // 12: vtgate.StreamExecuteMultiResponse
	(*vtgate.VStreamResponse)(nil),            // 13: vtgate.VStreamResponse
	(*vtgate.PrepareResponse)(nil),            // 14: vtgate.PrepareResponse
	(*vtgate.CloseSessionResponse)(nil),       // 15: vtgate.CloseSessionResponse
}
var file_vtgateservice_proto_depIdxs = []int32{
	0,  // 0: vtgateservice.Vitess.Execute:input_type -> vtgate.ExecuteRequest
	1,  // 1: vtgateservice.Vitess.ExecuteMulti:input_type -> vtgate.ExecuteMultiRequest
	2,  // 2: vtgateservice.Vitess.ExecuteBatch:input_type -> vtgate.ExecuteBatchRequest
	3,  // 3: vtgateservice.Vitess.StreamExecute:input_type -> vtgate.StreamExecuteRequest
	4,  // 4: vtgateservice.Vitess.StreamExecuteMulti:input_type -> vtgate.StreamExecuteMultiRequest
	5,  // 5: vtgateservice.Vitess.VStream:input_type -> vtgate.VStreamRequest
	6,  // 6: vtgateservice.Vitess.Prepare:input_type -> vtgate.PrepareRequest
	7,  // 7: vtgateservice.Vitess.CloseSession:input_type -> vtgate.CloseSessionRequest
	8,  // 8: vtgateservice.Vitess.Execute:output_type -> vtgate.ExecuteResponse
	9,  // 9: vtgateservice.Vitess.ExecuteMulti:output_type -> vtgate.ExecuteMultiResponse
	10, // 10: vtgateservice.Vitess.ExecuteBatch:output_type -> vtgate.ExecuteBatchResponse
	11, // 11: vtgateservice.Vitess.StreamExecute:output_type -> vtgate.StreamExecuteResponse
	12, // 12: vtgateservice.Vitess.StreamExecuteMulti:output_type -> vtgate.StreamExecuteMultiResponse
	13, // 13: vtgateservice.Vitess.VStream:output_type -> vtgate.VStreamResponse
	14, // 14: vtgateservice.Vitess.Prepare:output_type -> vtgate.PrepareResponse
	15, // 15: vtgateservice.Vitess.CloseSession:output_type -> vtgate.CloseSessionResponse
	8,  // [8:16] is the sub-list for method output_type
	0,  // [0:8] is the sub-list for method input_type
	0,  // [0:0] is the sub-list for extension type_name
	0,  // [0:0] is the sub-list for extension extendee
	0,  // [0:0] is the sub-list for field type_name
}

func init() { file_vtgateservice_proto_init() }
func file_vtgateservice_proto_init() {
	if File_vtgateservice_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_vtgateservice_proto_rawDesc), len(file_vtgateservice_proto_rawDesc)),
			NumEnums:      0,
			NumMessages:   0,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_vtgateservice_proto_goTypes,
		DependencyIndexes: file_vtgateservice_proto_depIdxs,
	}.Build()
	File_vtgateservice_proto = out.File
	file_vtgateservice_proto_goTypes = nil
	file_vtgateservice_proto_depIdxs = nil
}
