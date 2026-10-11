// Command gomodproxy writes a GOPROXY file tree for local module sources, so
// builds and installs resolve postgremq.dev modules without the vanity domain
// or a published version:
//
//	gomodproxy -out DIR postgremq.dev/postgremq-go@v0.2.0=SRC_DIR ...
//
// Module zips are built with golang.org/x/mod/zip, the algorithm the Go
// module proxy uses, so their go.sum hashes equal the published ones when
// SRC_DIR holds the module's files at its release tag. Use the tree with
// GOPROXY=file://DIR,https://proxy.golang.org and GONOSUMDB=postgremq.dev.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"golang.org/x/mod/modfile"
	"golang.org/x/mod/module"
	"golang.org/x/mod/zip"
)

func main() {
	out := flag.String("out", "", "proxy directory to write")
	flag.Parse()
	if *out == "" || flag.NArg() == 0 {
		fmt.Fprintln(os.Stderr, "usage: gomodproxy -out DIR module@version=SRC_DIR ...")
		os.Exit(2)
	}
	for _, arg := range flag.Args() {
		if err := add(*out, arg); err != nil {
			fmt.Fprintf(os.Stderr, "gomodproxy: %s: %v\n", arg, err)
			os.Exit(1)
		}
	}
}

func add(out, arg string) error {
	spec, src, ok := strings.Cut(arg, "=")
	if !ok {
		return fmt.Errorf("want module@version=SRC_DIR")
	}
	path, version, ok := strings.Cut(spec, "@")
	if !ok {
		return fmt.Errorf("want module@version=SRC_DIR")
	}
	mv := module.Version{Path: path, Version: version}
	if err := module.Check(path, version); err != nil {
		return err
	}
	gomod, err := os.ReadFile(filepath.Join(src, "go.mod"))
	if err != nil {
		return err
	}
	if declared := modfile.ModulePath(gomod); declared != path {
		return fmt.Errorf("%s/go.mod declares module %q", src, declared)
	}
	escPath, err := module.EscapePath(path)
	if err != nil {
		return err
	}
	escVersion, err := module.EscapeVersion(version)
	if err != nil {
		return err
	}
	dir := filepath.Join(out, filepath.FromSlash(escPath), "@v")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	var archive bytes.Buffer
	if err := zip.CreateFromDir(&archive, mv, src); err != nil {
		return err
	}
	info, err := json.Marshal(map[string]string{"Version": version, "Time": time.Now().UTC().Format(time.RFC3339)})
	if err != nil {
		return err
	}
	for name, data := range map[string][]byte{
		escVersion + ".zip":  archive.Bytes(),
		escVersion + ".mod":  gomod,
		escVersion + ".info": info,
	} {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o644); err != nil {
			return err
		}
	}
	return appendVersion(filepath.Join(dir, "list"), version)
}

func appendVersion(list, version string) error {
	data, err := os.ReadFile(list)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	versions := strings.Fields(string(data))
	if !slices.Contains(versions, version) {
		versions = append(versions, version)
	}
	return os.WriteFile(list, []byte(strings.Join(versions, "\n")+"\n"), 0o644)
}
