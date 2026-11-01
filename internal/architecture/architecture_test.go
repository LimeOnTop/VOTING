package architecture_test

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

// Rules describe the permitted direction of application dependencies. Concrete
// adapters meet at the composition root, never through service implementations.
var inward = map[string]map[string]bool{
	"apperr":        {},
	"entity":        {"apperr": true},
	"usecase":       {"entity": true, "apperr": true},
	"service":       {"entity": true, "usecase": true, "apperr": true},
	"controller":    {"entity": true, "usecase": true, "apperr": true},
	"cache":         {"entity": true, "usecase": true, "apperr": true},
	"repository":    {"entity": true, "usecase": true, "apperr": true},
	"client":        {"entity": true, "usecase": true, "apperr": true},
	"auth":          {"entity": true, "usecase": true, "apperr": true},
	"observability": {"entity": true, "usecase": true, "apperr": true},
	"app":           nil, // The composition root intentionally wires concrete adapters.
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate architecture test source")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("locate module relative to test source: %v", err)
	}
	return root
}
func modulePath(t *testing.T, root string) string {
	t.Helper()
	// #nosec G304 -- root comes only from this test source via runtime.Caller, never external input.
	content, err := os.ReadFile(filepath.Join(root, "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(content), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == "module" {
			return strings.Trim(fields[1], "\"")
		}
	}
	t.Fatal("module declaration not found")
	return ""
}
func isInner(layer string) bool {
	return layer == "entity" || layer == "usecase" || layer == "service" || layer == "apperr"
}
func hasPrefix(path, prefix string) bool {
	return path == prefix || strings.HasPrefix(path, prefix+"/")
}

func dependencyViolation(module, layer, path string) string {
	allowed, classified := inward[layer]
	if !classified || layer == "app" {
		return ""
	}
	if hasPrefix(path, module) {
		if layer == "repository" && hasPrefix(path, module+"/migrations") {
			return "" // Embedded SQL belongs to the persistence adapter.
		}
		prefix := module + "/internal/"
		if !strings.HasPrefix(path, prefix) {
			return "application core/adapters cannot import composition or command packages"
		}
		target := strings.Split(strings.TrimPrefix(path, prefix), "/")[0]
		if target == layer || allowed[target] {
			return ""
		}
		return fmt.Sprintf("%s must depend on an inward port, not concrete %s", layer, target)
	}
	// Domain/application packages remain independent of any external framework.
	// Adapters may use frameworks for their own responsibility only.
	first := strings.Split(path, "/")[0]
	if isInner(layer) && strings.Contains(first, ".") {
		return "domain and application layers cannot import external frameworks"
	}
	if layer == "service" && (hasPrefix(path, "net/http") || path == "net" || hasPrefix(path, "database/sql") || path == "os") {
		return "service cannot own transport, database or environment access"
	}
	if layer == "controller" || layer == "cache" || layer == "auth" || layer == "observability" {
		for _, prefix := range []string{"github.com/jackc/pgx", "github.com/segmentio/kafka-go", "database/sql"} {
			if hasPrefix(path, prefix) {
				return "adapter must use an inward repository/queue port"
			}
		}
	}
	if layer == "repository" && hasPrefix(path, "github.com/segmentio/kafka-go") {
		return "SQL repository cannot own a broker client"
	}
	if layer == "client" && (hasPrefix(path, "github.com/jackc/pgx") || hasPrefix(path, "database/sql")) {
		return "message client cannot own SQL persistence"
	}
	if layer == "controller" && hasPrefix(path, "github.com/prometheus") {
		return "controller must use its observer port, not a metrics implementation"
	}
	return ""
}

func TestLayersDependInward(t *testing.T) {
	root := repositoryRoot(t)
	module := modulePath(t, root)
	walkProduction(t, root, func(path, layer string, file *ast.File) {
		for _, spec := range file.Imports {
			imported, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				t.Fatalf("parse import in %s: %v", path, err)
			}
			if reason := dependencyViolation(module, layer, imported); reason != "" {
				t.Errorf("%s imports %s: %s", path, imported, reason)
			}
		}
	})
}

func TestNoImplicitDependencyInitialization(t *testing.T) {
	root := repositoryRoot(t)
	walkProduction(t, root, func(path, layer string, file *ast.File) {
		for _, decl := range file.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && fn.Name.Name == "init" {
				t.Errorf("%s: construct dependencies explicitly; init() hides wiring", path)
			}
			// Sentinel errors and compile-time interface assertions are fine. An inner
			// package must not initialize a database, client or clock at package load.
			group, ok := decl.(*ast.GenDecl)
			if !ok || group.Tok != token.VAR || !isInner(layer) {
				continue
			}
			for _, spec := range group.Specs {
				values, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}
				assertions := true
				for _, name := range values.Names {
					if name.Name != "_" {
						assertions = false
					}
				}
				if assertions {
					continue
				}
				for _, value := range values.Values {
					ast.Inspect(value, func(node ast.Node) bool {
						call, ok := node.(*ast.CallExpr)
						if !ok {
							return true
						}
						if selector, ok := call.Fun.(*ast.SelectorExpr); ok {
							if pkg, ok := selector.X.(*ast.Ident); ok && pkg.Name == "errors" && selector.Sel.Name == "New" {
								return false
							}
						}
						t.Errorf("%s: callable package-variable initializer in %s; inject runtime dependencies through constructors", path, layer)
						return false
					})
				}
			}
		}
	})
}

func walkProduction(t *testing.T, root string, inspect func(string, string, *ast.File)) {
	t.Helper()
	internal := filepath.Join(root, "internal")
	err := filepath.WalkDir(internal, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(internal, path)
		if err != nil {
			return err
		}
		layer := strings.Split(filepath.ToSlash(relative), "/")[0]
		_, known := inward[layer]
		if entry.IsDir() {
			if relative != "." && !known {
				return filepath.SkipDir
			}
			return nil
		}
		if !known || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		label, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		inspect(filepath.ToSlash(label), layer, file)
		return nil
	})
	if err != nil {
		t.Fatalf("inspect architecture imports: %v", err)
	}
}
