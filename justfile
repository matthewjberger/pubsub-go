set windows-shell := ["powershell.exe"]

# Lists all available recipes
@just:
    just --list

# Runs all tests
test:
    go test ./...

# go vet + gofmt -l (fails if anything is unformatted) (Windows)
[windows]
check:
    go vet ./...
    $unformatted = (gofmt -l . | Out-String).Trim(); if ($unformatted) { Write-Host $unformatted; exit 1 }

# go vet + gofmt -l (fails if anything is unformatted) (Unix)
[unix]
check:
    go vet ./...
    unformatted=$(gofmt -l .); if [ -n "$unformatted" ]; then echo "$unformatted"; exit 1; fi

# gofmt -w .
format:
    gofmt -w .

# go mod tidy
tidy:
    go mod tidy

# Shows what `go mod tidy` would change
tidy-check:
    go mod tidy -diff

# Deps with available updates
outdated:
    go list -m -u all

# check + test (run before pushing)
ci: check test

# Full read-only audit
audit: check tidy-check outdated test

# Builds all three binaries with the OS-default extension
build:
    go build ./cmd/broker
    go build ./cmd/publisher
    go build ./cmd/subscriber

# End-to-end demo: broker + publisher + subscriber in one shell (Ctrl-C to exit) (Windows)
[windows]
demo: build
    $broker = Start-Process -FilePath ".\broker.exe" -NoNewWindow -PassThru
    Start-Sleep -Milliseconds 500
    $publisher = Start-Process -FilePath ".\publisher.exe" -NoNewWindow -PassThru
    try { & ".\subscriber.exe" } finally { Stop-Process -Id $broker.Id -Force -ErrorAction SilentlyContinue; Stop-Process -Id $publisher.Id -Force -ErrorAction SilentlyContinue }

# End-to-end demo: broker + publisher + subscriber in one shell (Ctrl-C to exit) (Unix)
[unix]
demo: build
    ./broker & broker_pid=$!; sleep 0.5; ./publisher & publisher_pid=$!; trap "kill $broker_pid $publisher_pid 2>/dev/null" EXIT INT TERM; ./subscriber

# Runs the broker (default 127.0.0.1:9000)
broker *args:
    go run ./cmd/broker {{args}}

# Runs the publisher demo
publisher *args:
    go run ./cmd/publisher {{args}}

# Runs the subscriber demo
subscriber *args:
    go run ./cmd/subscriber {{args}}

# Renders package doc for ./pubsub
doc:
    go doc -all ./pubsub

# Removes built binaries (Windows)
[windows]
clean:
    Remove-Item -Force -ErrorAction SilentlyContinue broker.exe,publisher.exe,subscriber.exe

# Removes built binaries (Unix)
[unix]
clean:
    rm -f broker publisher subscriber

# Displays Go tool version
@versions:
    go version
