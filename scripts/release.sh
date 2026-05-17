#!/usr/bin/env bash
# Usage: ./scripts/release.sh [major|minor|patch]
# Default: patch
set -euo pipefail

if ! command -v gh &>/dev/null; then
  echo "Error: gh CLI is not installed. See https://cli.github.com"
  exit 1
fi

BUMP="${1:-patch}"
DIR="$(cd "$(dirname "$0")/.." && pwd)"

if ! git -C "$DIR" diff --quiet || ! git -C "$DIR" diff --cached --quiet; then
  echo "Error: uncommitted changes present"
  exit 1
fi

LATEST="$(git -C "$DIR" tag --sort=-version:refname | grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$' | head -1 || true)"
LATEST="${LATEST:-v0.0.0}"

IFS='.' read -r MAJOR MINOR PATCH <<< "${LATEST#v}"

case "$BUMP" in
  major) MAJOR=$((MAJOR + 1)); MINOR=0; PATCH=0 ;;
  minor) MINOR=$((MINOR + 1)); PATCH=0 ;;
  patch) PATCH=$((PATCH + 1)) ;;
  *) echo "Usage: $0 [major|minor|patch]"; exit 1 ;;
esac

VERSION="v${MAJOR}.${MINOR}.${PATCH}"
echo "Releasing ligo-microservices: $LATEST → $VERSION"

BRANCH="$(git -C "$DIR" branch --show-current)"
git -C "$DIR" tag -a "$VERSION" -m "$VERSION"
git -C "$DIR" push origin "$BRANCH"
git -C "$DIR" push origin "$VERSION"

gh release create "$VERSION" --repo linkeunid/ligo-microservices --title "$VERSION" --notes "" --latest

echo "Released ligo-microservices $VERSION"
