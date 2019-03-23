package main

import (
	"crypto/sha256"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
)

func main() {
	inDir := flag.String("in", "", "Where the source files are")
	flag.Parse()

	if *inDir == "" {
		flag.Usage()
		os.Exit(1)
	}

	if err := dedup(*inDir); err != nil {
		fmt.Fprint(os.Stderr, "Error doing dedup: "+err.Error())
		os.Exit(1)
	}
}

func dedup(inDir string) error {
	if cleanDir, err := filepath.Abs(inDir); err == nil {
		inDir = cleanDir
	} else {
		return err
	}

	h := sha256.New()

	hashes := map[string]string{}

	filepath.Walk(inDir, func(path string, info os.FileInfo, err error) error {
		relPath := strings.TrimPrefix(path, inDir+string(os.PathSeparator))
		if info.IsDir() {
			return nil
		}
		if info.Size() == 0 {
			return nil
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()

		h.Reset()
		if _, err := io.Copy(h, f); err != nil {
			return err
		}
		hash := fmt.Sprintf("%x", h.Sum(nil))
		if first, found := hashes[hash]; found {
			log.Print("Found duplicate: " + relPath)
			log.Print("  Original: " + first)
			return nil
		}
		hashes[hash] = relPath

		return nil
	})

	return nil
}
