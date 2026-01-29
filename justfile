# print this overview
@default:
  just --list

changelog := "CHANGELOG.md"

# check formatting and run clippy
[group('Linting')]
@check:
  cargo fmt --check
  cargo clippy

# run doc tests
[group('Testing')]
@test-doc:
  cargo test --doc

# run all tests and collect coverage information
[group('Testing')]
@test format="--json" : test-doc
  cargo llvm-cov --all-targets --examples --cobertura --output-path coverage.xml
  cargo llvm-cov report {{format}} --summary-only 

# generate a new changelog entry
[group('Release')]
@log-changes tag:
  git cliff -o {{changelog}} -t {{tag}}

# publish the current state of affairs
[group('Release')]
@publish: check test
  cargo publish
  
