package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/evanphx/json-patch"
	"github.com/krishicks/yaml-patch"
	"vitess.io/vitess/go/vt/log"
	yaml "gopkg.in/yaml.v2"
)

var (
	baseYamlFile = flag.String("base_yaml", "docker-compose.base.yml", "Starting docker-compose yaml")
	baseVschemaFile = "base_vschema.json"
	tablets = flag.Int("tablets", 2, "Number of tablets")
	shards = flag.Int("shards", 1, "How many shards to configure")
)

func main() {
	args := os.Args[1:]
	dockerYaml, err := ioutil.ReadFile(*baseYamlFile)
	if err != nil {
		log.Fatalf("reading base yaml file %s: %s", *baseYamlFile, err)
	}



	//dockerYaml = applyPatch(dockerYaml, consulPatchFile)

	fmt.Println(string(dockerYaml))

	// Write Final docker-compose.yml
	err = ioutil.WriteFile("docker-compose.yml", dockerYaml, 0644)
	if err != nil {
		log.Fatalf("writing docker-compose.yml %s", err)
	}

	//for i := 0; i < *shards; i++ {
	//	shard := "-"
	//	if i != 0 {
	//		shard = "thing-"
	//	}
	//
	//}

	// Let's create a merge patch from these two documents...
	original := []byte(`{"name": "John", "age": 24, "height": 3.21}`)
	target := []byte(`{"name": "Jane", "age": 24}`)

	jpatch, err := jsonpatch.CreateMergePatch(original, target)
	if err != nil {
		panic(err)
	}

	// Now lets apply the patch against a different JSON document...

	alternative := []byte(`{"name": "Tina", "age": 28, "height": 3.75}`)
	modifiedAlternative, err := jsonpatch.MergePatch(alternative, jpatch)

	fmt.Printf("patch document:   %s\n", jpatch)
	fmt.Printf("updated alternative doc: %s\n", modifiedAlternative)
}

func applyPatch(dockerYaml []byte, patchFile string) []byte {
	yamlPatch, err := ioutil.ReadFile(patchFile)
	if err != nil {
		log.Fatalf("reading yaml patch file %s: %s", patchFile, err)
	}

	patch, err := yamlpatch.DecodePatch(yamlPatch)
	if err != nil {
		log.Fatalf("decoding patch failed: %s", err)
	}

	bs, err := patch.Apply(dockerYaml)
	if err != nil {
		log.Fatalf("applying patch failed: %s", err)
	}
	return bs
}