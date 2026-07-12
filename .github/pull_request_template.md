## Summary

<!-- Brief description of what this PR changes and why. -->

## Rationale

<!-- Why is this change needed? Link any relevant issues or discussions. -->

## Checklist

- [ ] `cargo fmt --all -- --check` is clean
- [ ] `cargo check --locked --workspace --all-targets` passes
- [ ] `cargo clippy --locked --workspace --all-targets -- -D warnings` passes
- [ ] `cargo test --locked --workspace --all-targets` passes
- [ ] PyO3 package builds and `import ferry._native` succeeds (if `crates/ferry-python` is touched)

## Breaking changes

<!-- Describe any breaking changes, or write "None". -->

## Migration notes

<!-- If breaking, document how downstream users should migrate. -->

## Follow-ups

<!-- List any follow-up issues, TODOs, or deferred work. -->