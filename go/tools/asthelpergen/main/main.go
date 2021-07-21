package main

import (
	"flag"
	"log"

	. "vitess.io/vitess/go/tools/asthelpergen"
)

func main() {
	var patterns TypePaths
	var generate, except string
	var verify bool

	flag.Var(&patterns, "in", "Go packages to load the generator")
	flag.StringVar(&generate, "iface", "", "Root interface generate rewriter for")
	flag.BoolVar(&verify, "verify", false, "ensure that the generated files are correct")
	flag.StringVar(&except, "except", "", "don't deep clone these types")
	flag.Parse()

	result, err := GenerateASTHelpers(patterns, generate, except)
	if err != nil {
		log.Fatal(err)
	}

	if verify {
		for _, err := range VerifyFilesOnDisk(result) {
			log.Fatal(err)
		}
		log.Printf("%d files OK", len(result))
	} else {
		for fullPath, file := range result {
			if err := file.Save(fullPath); err != nil {
				log.Fatalf("failed to save file to '%s': %v", fullPath, err)
			}
			log.Printf("saved '%s'", fullPath)
		}
	}
}
