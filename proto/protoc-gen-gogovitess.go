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

	vanity.ForEachFieldInFilesExcludingExtensions(vanity.OnlyProto2(files), vanity.TurnOffNullableForNativeTypesWithoutDefaultsOnly)
	vanity.ForEachFile(files, vanity.TurnOffGoUnrecognizedAll)
	vanity.ForEachFile(files, vanity.TurnOffGoUnkeyedAll)
	vanity.ForEachFile(files, vanity.TurnOffGoSizecacheAll)

	vanity.ForEachFile(files, vanity.TurnOffGoEnumPrefixAll)
	vanity.ForEachFile(files, vanity.TurnOffGoEnumStringerAll)
	vanity.ForEachFile(files, vanity.TurnOnEnumStringerAll)

	vanity.ForEachFile(files, vanity.TurnOnEqualAll)
	vanity.ForEachFile(files, vanity.TurnOnGoStringAll)
	vanity.ForEachFile(files, vanity.TurnOffGoStringerAll)
	vanity.ForEachFile(files, vanity.TurnOnStringerAll)

	resp := command.Generate(req)
	command.Write(resp)
}

func TurnOnProtoSizerAll(file *descriptor.FileDescriptorProto) {
	vanity.SetBoolFileOption(gogoproto.E_ProtosizerAll, true)(file)
}
