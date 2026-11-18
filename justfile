# print this overview
@default:
  just --list

# check formatting, run clippy on every target and build the docs, warnings as errors
[group('Linting')]
@check:
  cargo fmt --check
  cargo clippy --all-targets -- -D warnings
  RUSTDOCFLAGS="-D warnings" cargo doc --no-deps

# build the library on the declared rust-version (needs `rustup toolchain install 1.88`)
[group('Linting')]
@msrv:
  cargo +1.88 check --locked

# run doc tests
[group('Testing')]
@test-doc:
  cargo test --doc

# run all tests and collect coverage information
[group('Testing')]
@test format="--json" : test-doc
  cargo llvm-cov --all-targets --examples --cobertura --output-path coverage.xml
  cargo llvm-cov report {{format}} --summary-only

# package and verify the crate the way crates.io will see it, without uploading
[group('Release')]
@package *args:
  cargo package {{args}}
