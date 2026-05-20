# Contributing to pubsub-go

## Current Status

`pubsub-go` is small on purpose and is not actively soliciting external contributions. Bug reports and protocol-level interop questions are welcome via the issue tracker; PRs that grow the surface area (bridging, persistence, auth, wildcard topics) will likely be declined.

## Local development

```powershell
just check     # go vet + gofmt -l
just test      # go test ./...
just ci        # check + test, run before pushing
just audit     # check + tidy diff + outdated + test
```

The library has no external Go module dependencies. `go.mod` should stay at zero require entries beyond the Go version directive.

## Code style

- Methods exist only for Go-standard lifecycle (`Close`, `Shutdown`) and trivial accessors (`ID`, `Address`). Everything else is a package-level function that takes the state as its first argument.
- Wire types in `pubsub/protocol.go` are plain data. Adding a method to `PeerEvent` or `BrokerMessage` is a no-go; that would couple the wire schema to Go.
- Receiver names follow Go convention: `b *Broker`, `c *Client`. Field and local names spell things out (`subscribers`, `message`, `outbound`) rather than abbreviating.
- No comments that just restate the code. Comments explain *why* something is done a particular way (the stale-disconnect token is the canonical example).

## License

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in `pubsub-go` by you, as defined in the Apache-2.0 license, shall be dual licensed as MIT and Apache-2.0, without any additional terms or conditions. See [`docs/LICENSING.md`](LICENSING.md).
