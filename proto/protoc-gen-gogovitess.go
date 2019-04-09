package main

import (
 "github.com/gogo/protobuf/vanity"
 "github.com/gogo/protobuf/vanity/command"

 "github.com/gogo/protobuf/gogoproto"
 descriptor "github.com/gogo/protobuf/protoc-gen-gogo/descriptor"
)

func main() {
 req := command.Read()
 files := req.GetProtoFile()
 files = vanity.FilterFiles(files, vanity.NotGoogleProtobufDescriptorProto)

 vanity.ForEachFile(files, vanity.TurnOnMarshalerAll)
 vanity.ForEachFile(files, TurnOnProtoSizerAll)
 vanity.ForEachFile(files, vanity.TurnOnUnmarshalerAll)

 resp := command.Generate(req)
 command.Write(resp)
}

func TurnOnProtoSizerAll(file *descriptor.FileDescriptorProto) {
 vanity.SetBoolFileOption(gogoproto.E_ProtosizerAll, true)(file)
}
