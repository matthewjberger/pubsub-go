# Licensing

`pubsub-go` is dual-licensed under MIT and Apache-2.0 at your option. Either license is sufficient to use, modify, or redistribute the library; you do not need to comply with both. Projects that depend on `pubsub-go` can be released under any license the author chooses, including closed-source commercial.

The full text of each license is in [`LICENSE-MIT`](../LICENSE-MIT) and [`LICENSE-APACHE`](../LICENSE-APACHE) at the repository root.

## Why dual MIT + Apache-2.0

Offering both licenses lets a downstream pick whichever fits its own obligations. A project that needs an explicit patent grant takes Apache-2.0; a project that wants the shortest possible notice takes MIT. Neither choice asks anything of the other.

- **MIT** is the simplest permissive license: a short notice, no patent grant.
- **Apache-2.0** adds an explicit patent grant from contributors, an explicit attribution requirement, and a `NOTICE` carry-forward convention.

Picking either license is a downstream choice; consumers should retain the corresponding `LICENSE-*` file in their distribution.

## Dependencies

`pubsub-go` depends only on the Go standard library. There are no third-party Go modules to attribute or audit. `go mod tidy` should leave `go.mod` with zero require entries beyond the Go version directive.

If you fork or vendor this code, your fork inherits the dual license unless you remove one of the `LICENSE-*` files and re-license your fork explicitly under the remaining one.

## Contributions

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in `pubsub-go` by you, as defined in the Apache-2.0 license, shall be dual licensed as MIT and Apache-2.0, without any additional terms or conditions.
