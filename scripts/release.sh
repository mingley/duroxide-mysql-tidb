#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: ./scripts/release.sh <version> [--push]

Examples:
  ./scripts/release.sh 0.1.0 --push   # Tag current 0.1.0 and push tag to trigger Release workflow
  ./scripts/release.sh 0.1.1 --push   # Bump Cargo.toml/Cargo.lock to 0.1.1, commit, tag v0.1.1, and push
EOF
}

if [[ $# -lt 1 || $# -gt 2 ]]; then
  usage
  exit 1
fi

NEW_VERSION="${1#v}"
PUSH_FLAG="${2:-}"

if [[ ! "$NEW_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]]; then
  echo "error: invalid semver version '$NEW_VERSION'" >&2
  exit 1
fi

if [[ -n "$PUSH_FLAG" && "$PUSH_FLAG" != "--push" ]]; then
  usage
  exit 1
fi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

if [[ -n "$(git status --porcelain)" ]]; then
  echo "error: working tree is not clean; commit or stash changes first" >&2
  exit 1
fi

TAG="v${NEW_VERSION}"
if git rev-parse "$TAG" >/dev/null 2>&1; then
  echo "error: tag '$TAG' already exists" >&2
  exit 1
fi

CURRENT_VERSION="$(cargo metadata --no-deps --format-version 1 | python3 -c 'import json,sys; print(json.load(sys.stdin)["packages"][0]["version"])')"

if [[ "$CURRENT_VERSION" != "$NEW_VERSION" ]]; then
  echo "Bumping version: ${CURRENT_VERSION} -> ${NEW_VERSION}"
  python3 - "$NEW_VERSION" <<'PY'
import pathlib, re, sys
ver = sys.argv[1]
p = pathlib.Path("Cargo.toml")
txt = p.read_text()
updated, n = re.subn(r'(?m)^version\s*=\s*"[^"]+"', f'version = "{ver}"', txt, count=1)
if n != 1:
    raise SystemExit("failed to update version in Cargo.toml")
p.write_text(updated)
PY
  cargo check --quiet
fi

echo "Running pre-release verification..."
cargo fmt --all -- --check
cargo check --no-default-features --features memory
cargo clippy --all-targets --all-features -- -D warnings
cargo test --lib --all-features
cargo test --doc --all-features
cargo test --test memory_provider_validations --all-features
cargo package --allow-dirty

if [[ -n "$(git status --porcelain Cargo.toml Cargo.lock)" ]]; then
  git add Cargo.toml Cargo.lock
  git commit -m "Release ${TAG}"
fi

git tag -a "$TAG" -m "Release ${TAG}"
echo "Created tag ${TAG}"

if [[ "$PUSH_FLAG" == "--push" ]]; then
  git push origin main "$TAG"
  echo "Pushed main and ${TAG} to origin"
else
  echo "Dry tag created locally. Run 'git push origin main ${TAG}' when ready."
fi
