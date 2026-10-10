# Local HOCON parser patch

Source: crates.io `hocon` 0.9.0, MIT license (see LICENSE).
Only library source and its manifest/documentation are vendored.

`loader_config.rs` trims document-end whitespace before appending the parser's
newline/NUL sentinel. Upstream's strict parser otherwise rejects a valid document
ending in a newline followed by spaces or tabs. File loading and relative include
resolution remain intact. Our `tests/persistence.rs` and process restart tests
cover indented configuration files and preserve strict parse failures.

The crate's warning lints are allowed locally, matching Cargo's lint cap for the
registry dependency on newer Rust compilers. Its prohibition of unsafe code is
retained. Application lint settings are unchanged.
