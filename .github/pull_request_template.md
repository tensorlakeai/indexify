## Context

<!--
In a few sentences or less, please explain the context behind this change to help answer why this change is needed.

If this is a bug fix, make sure to include "fixes #xxxx", or
"closes #xxxx".

Screenshots, logs, code or other visual aids are greatly appreciated.
 -->

## What

<!--
In a few sentences, please summarize the change to help reviewers.

Consider providing screenshots, logs, code or other visual aids to help the reviewer understand the approach taken.
-->

## Testing

<!--
Please include steps used to verify the change.

Consider providing screenshots, logs, code or other visual aids to help the reviewer in their testing.
-->

## Contribution Checklist

- [ ] If a Python package was changed, please run `make fmt` in the package directory.
- [ ] If the server was changed, please run `make fmt` in `server/`.
- [ ] Make sure all PR Checks are passing.
<!--
Notes:

Tests of a Python package can be run manually. Start a Server and an Executor then
run `make test` in the Python package directory.

To test if changes to the server are backward compatible with the latest
release, label the PR with `ci_compat_test`. This might report failures
unrelated to your change if previous incompatible changes were pushed without
being released yet
-->
