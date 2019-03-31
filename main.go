package main

import (
	"crypto/sha256"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/afero"
)

func main() {
	inDir := flag.String("in", "", "Where the source files are")
	outDir := flag.String("out", "", "Where the deduplicated files will go")
	verbose := flag.Bool("v", false, "Verbose output")
	flag.Parse()

	if *inDir == "" || *outDir == "" {
		flag.Usage()
		os.Exit(1)
	}

	copied, skipped, err := dedup(*inDir, *outDir, *verbose)
	if err != nil {
		fmt.Fprint(os.Stderr, "Error doing dedup: "+err.Error())
		os.Exit(1)
	}

	if copied == 0 {
		fmt.Println("Nothing happened?")
		os.Exit(1)
	} else {
		percent := float64(copied) / float64(copied+skipped)
		fmt.Printf("Copied %d MB, skipped %d MB (%.2f%%)\n", copied/1024, skipped/1024, percent)
	}
}

func dedup(inDir string, outDir string, verbose bool) (int64, int64, error) {
	return dedupFS(afero.NewOsFs(), inDir, outDir, verbose)
}

func dedupFS(fs afero.Fs, inDir string, outDir string, verbose bool) (int64, int64, error) {
	// var verboseLog *log.Logger
	verboseLog := ioutil.Discard
	if verbose {
		verboseLog = os.Stdout
	}
	if cleanDir, err := filepath.Abs(inDir); err == nil {
		inDir = cleanDir
	} else {
		return 0, 0, err
	}
	if cleanDir, err := filepath.Abs(outDir); err == nil {
		outDir = cleanDir
	} else {
		return 0, 0, err
	}

	h := sha256.New()

	hashes := map[string]string{}
	var copied int64
	var skipped int64

	err := afero.Walk(fs, inDir, func(sourcePath string, info os.FileInfo, err error) error {
		if err != nil {
			return errors.New("error during filesystem walk: " + err.Error())
		}

		relPath := strings.TrimPrefix(sourcePath, inDir+string(os.PathSeparator))
		if info.IsDir() || info.Size() == 0 || strings.HasSuffix(sourcePath, ".DS_Store") {
			return nil
		}
		fmt.Fprintf(verboseLog, "% 50s: ", relPath)
		inFile, err := fs.Open(sourcePath)
		if err != nil {
			return errors.New("failed to open source file: " + err.Error())
		}
		defer inFile.Close()

		h.Reset()
		if _, err := io.Copy(h, inFile); err != nil {
			return errors.New("failed to read/hash file: " + err.Error())
		}
		hash := fmt.Sprintf("%x", h.Sum(nil))
		if _, found := hashes[hash]; found {
			fmt.Fprintln(verboseLog, "skip")
			skipped += info.Size()
			return nil
		}
		hashes[hash] = relPath

		if _, err := inFile.Seek(0, 0); err != nil {
			return errors.New("failed to seek source file: " + err.Error())
		}

		dir := filepath.Join(outDir, filepath.Dir(relPath))
		filename := filepath.Base(relPath)
		if err := fs.MkdirAll(dir, 0770); err != nil {
			return errors.New("failed to create directory heirarchy: " + err.Error())
		}
		outFullPath := filepath.Join(dir, filename)
		outFile, err := fs.Create(outFullPath)
		if err != nil {
			return errors.New("failed to create output file: " + err.Error())
		}
		defer func() {
			if err := outFile.Close(); err != nil {
				// less of a problem with the Sync call
				log.Printf("Failed to close file %q: %s", outFullPath, err.Error())
			}
		}()
		fmt.Fprintln(verboseLog, "copying")
		if _, err := io.Copy(outFile, inFile); err != nil {
			return errors.New("failed to copy file contents: " + err.Error())
		}
		// not sure if needed
		if err := outFile.Sync(); err != nil {
			return errors.New("failed to sync file contents: " + err.Error())
		}
		copied += info.Size()

		return nil
	})
	if err != nil {
		err = errors.New("failed somewhere along the copy process: " + err.Error())
	}

	return copied, skipped, err
}
