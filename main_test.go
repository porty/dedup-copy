package main

import (
	"os"
	"runtime"
	"strings"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

func normalise(path string) string {
	if runtime.GOOS != "windows" {
		return path
	}
	return "C:" + strings.Replace(path, "/", "\\", -1)
}

func TestDedupFSUnix(t *testing.T) {
	fs := afero.NewMemMapFs()
	writeFile := func(filename string, contents string) {
		filename = normalise(filename)
		if err := afero.WriteFile(fs, filename, []byte(contents), 0660); err != nil {
			panic(err)
		}
	}

	writeFile("/src/dir1/apple.txt", "apple")
	writeFile("/src/dir1/banana.txt", "banana")
	writeFile("/src/dir2/apple.txt", "apple")
	writeFile("/src/dir2/carrot.txt", "carrot")
	writeFile("/src/dir2/empty.txt", "")
	writeFile("/src/dir2/.DS_Store", "ds store")
	writeFile("/src/dir2/._.DS_Store", "more ds store")

	copied, skipped, err := dedupFS(fs, normalise("/src"), normalise("/dest"), true)

	require.NoError(t, err)
	require.Equal(t, int64(17), copied)
	require.Equal(t, int64(5), skipped)

	destFiles := []string{}

	afero.Walk(fs, normalise("/dest"), func(sourcePath string, info os.FileInfo, err error) error {
		if err != nil {
			panic(err)
		}
		if info.IsDir() {
			return nil
		}

		contents, err := afero.ReadFile(fs, sourcePath)
		if err != nil {
			panic(err)
		}
		destFiles = append(destFiles, sourcePath+"|"+string(contents))
		return nil
	})

	require.Equal(t, []string{
		normalise("/dest/dir1/apple.txt|apple"),
		normalise("/dest/dir1/banana.txt|banana"),
		normalise("/dest/dir2/carrot.txt|carrot"),
	}, destFiles)
}
