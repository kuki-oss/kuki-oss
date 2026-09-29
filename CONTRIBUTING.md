# Contributing to Kuki

Thanks for your interest in Kuki OSS!

## How we work

Kuki OSS is licensed under AGPL-3.0. Contributions are accepted under the
same license, and you keep the copyright in your work. We use a DCO (sign-off)
instead of a CLA.

Accepted contributions are merged into Kuki OSS with your authorship preserved
and remain AGPL-licensed. Kuki Core (our closed-source version of Kuki) is developed
separately, and we do not copy contributed code into it. Where Core needs similar
functionality, we write our own implementation.

For changes to Core's engine components, we may implement the change ourselves, using
your PR as a detailed specification, instead of merging it. We will credit you in the
commit or release notes when we do.

## How this repository works

This GitHub repository is a **read-only mirror**. The source of truth is
hosted on our [infrastructure](https://source.kuki.co.ke/kuki-org/kuki-oss)

Open pull requests here on GitHub as normal. Maintainers review them on GitHub, 
then apply accepted changes to our source by hand, and the mirror publishes them 
back here. Because of this, your PR will be **closed rather than merged** 
with a comment linking to the commit that includes your work. Your authorship
is preserved.

## Ways to contribute

- **Report bugs or request features** by opening a GitHub issue.
- **Improve the docs**, since typo and clarity fixes are welcome.
- **Fix a bug or build a feature** via pull request. For anything
  non-trivial, please open an issue first so we can agree on the approach
  before you invest time.

## Submitting a pull request

1. Fork the repository and create a branch from `master`.
2. Make your change. Keep PRs focused, with one logical change per PR.
3. Add or update tests and docs where relevant.
4. Sign off every commit (see below).
5. Open a PR against `master` and describe what and why.
6. Keep your branch rebased on `master`. Because merging happens on our 
   infrastructure, PRs that fall behind may need a rebase before we can 
   apply them.

## Developer Certificate of Origin (DCO)

We use the [DCO](https://developercertificate.org/) instead of a CLA. By
signing off, you certify that you wrote the code or have the right to
submit it under this project's license.

Add a sign-off line to each commit with the `-s` flag:

```bash
git commit -s -m "Fix login redirect"
```

This appends:

```
Signed-off-by: Your Name <you@example.com>
```

The name and email must match your git config. To fix commits you already
made:

```bash
git commit --amend -s              # latest commit only
git rebase --signoff upstream/master # all commits on your branch
git push --force-with-lease
```

The DCO check on your PR will fail until every commit is signed off.

## Before you start

Contributions implementing Kuki Core functionality (cross-tenant
aggregation, ML ranking) won't be merged, regardless of quality. That logic
doesn't belong in this repository. See
[What's Not Included](https://docs.kuki.co.ke/architecture/whats-not-included).

We're especially interested in bug fixes (ingestion pipeline and ETL
worker), documentation, xlsx/PDF upload support, and self-host tooling.

## Development setup

Follow the [Quickstart](https://docs.kuki.co.ke/quickstart), then before
opening a PR run:

```bash
# backend
cargo fmt --all
cargo clippy --all-targets
cargo check --workspace
cargo test --workspace
```

## Review policy

This project currently has one maintainer, so review bandwidth is limited. PRs or
issues that don't follow this guide, lack tests where reasonable, or fall
outside project scope may be closed without extensive discussion. Bug fixes
with a regression test are reviewed fastest.

## Code style and tests

Before opening a PR, run:

```bash
cargo fmt --all
cargo clippy --all-targets -- -D warnings
cargo test
```

PRs should pass all three.

## Keeping your branch up to date

On a fork, `origin` is your copy. Add the main repository as `upstream`
once:

```bash
git remote add upstream https://github.com/kuki-oss/kuki-oss.git
```

Then, before opening or updating a PR:

```bash
git fetch upstream
git rebase --signoff upstream/master
git push --force-with-lease
```

## Security issues

Please **do not** report vulnerabilities in public issues or PRs. See
[SECURITY.md](SECURITY.md).

## Conduct

Be respectful and constructive. See [CODE OF CONDUCT](CODE_OF_CONDUCT.md)
