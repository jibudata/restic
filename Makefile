.PHONY: all clean test restic

all: restic

restic:
	go run build.go

restic-debug:
	go run -tags debug build.go

clean:
	rm -f restic

test:
	go test ./cmd/... ./internal/...

