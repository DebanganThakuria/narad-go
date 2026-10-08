package prometheus

import (
	"bufio"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"

	narad "github.com/debanganthakuria/narad-go"
)

const rootModule = "github.com/debanganthakuria/narad-go"

// releaseVersion is a plain tagged release, vMAJOR.MINOR.PATCH, with no
// pre-release or pseudo-version suffix.
var releaseVersion = regexp.MustCompile(`^v(\d+)\.(\d+)\.(\d+)$`)

// The replace in go.mod applies only inside this repository: Go drops a
// dependency's replace directives, so an importer resolves the client at
// exactly the version this module requires. That version has to be a
// real release, or go get of this module fails for anyone who does not
// already require the client, and it is the oldest client release an
// importer can end up with, so it must not name one newer than the
// client in this tree, which is the one these metrics are tested
// against.
func TestRequiresAReleasedClient(t *testing.T) {
	required := requiredVersion(t, "go.mod", rootModule)

	got := releaseVersion.FindStringSubmatch(required)
	if got == nil {
		t.Fatalf("go.mod requires %s %s; want a tagged release vX.Y.Z, since importers ignore the replace",
			rootModule, required)
	}
	current := releaseVersion.FindStringSubmatch("v" + narad.Version)
	if current == nil {
		t.Fatalf("narad.Version %q is not a release version", narad.Version)
	}
	if compareRelease(got[1:], current[1:]) > 0 {
		t.Fatalf("go.mod requires %s %s, newer than the client in this tree (v%s)",
			rootModule, required, narad.Version)
	}
}

// requiredVersion returns the version path is required at in the go.mod
// file at name, in either the single-line or the block form.
func requiredVersion(t *testing.T, name, path string) string {
	t.Helper()
	f, err := os.Open(name)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	inBlock := false
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if i := strings.Index(line, "//"); i >= 0 {
			line = line[:i]
		}
		fields := strings.Fields(line)
		switch {
		case len(fields) == 0:
			continue
		case fields[0] == "require" && len(fields) == 2 && fields[1] == "(":
			inBlock = true
			continue
		case inBlock && fields[0] == ")":
			inBlock = false
			continue
		case fields[0] == "require" && len(fields) == 3:
			fields = fields[1:]
		case !inBlock:
			continue
		}
		if len(fields) == 2 && fields[0] == path {
			return fields[1]
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	t.Fatalf("%s does not require %s", name, path)
	return ""
}

// compareRelease compares two [major, minor, patch] triples.
func compareRelease(a, b []string) int {
	for i := range a {
		x, _ := strconv.Atoi(a[i])
		y, _ := strconv.Atoi(b[i])
		if x != y {
			if x < y {
				return -1
			}
			return 1
		}
	}
	return 0
}
