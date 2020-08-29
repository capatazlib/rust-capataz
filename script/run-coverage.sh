#!/bin/bash

# Flags bellow help us make sure that the compiler optimizations are not bugging
# our coverage report
export CARGO_INCREMENTAL=0
export RUSTFLAGS="-Zprofile -Ccodegen-units=1 -Copt-level=0 -Clink-dead-code -Coverflow-checks=off -Zpanic_abort_tests -Cpanic=abort"
export RUSTDOCFLAGS="-Cpanic=abort"

cargo build --verbose "$CARGO_OPTIONS"
cargo test --verbose "$CARGO_OPTIONS"

# shellcheck disable=SC2046
zip -0 ./target/ccov.zip $(find . \( -name "capataz*.gc*" \) -print);

# build the report required by codecov
grcov ./target/ccov.zip -s . -t lcov --llvm --branch --ignore-not-existing --ignore "/*" -o ./target/lcov.info;
