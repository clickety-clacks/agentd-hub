#!/usr/bin/env bash
set -euo pipefail

export LC_ALL=C
umask 022

AGENTD_HUB_REPO_ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)
AGENTD_HUB_RELEASE_VERSION=0.1.0
AGENTD_HUB_BINARY="$AGENTD_HUB_REPO_ROOT/target/release/agentd-hub"
AGENTD_HUB_OUTPUT_DIR="$AGENTD_HUB_REPO_ROOT/target/release-assets"
AGENTD_HUB_SOURCE_DATE_EPOCH=
AGENTD_HUB_DRY_RUN=false

agentd_hub_usage() {
  echo "usage: scripts/package-release.sh [--dry-run] [--binary PATH] [--output-dir DIR] [--source-date-epoch SECONDS]" >&2
  exit 2
}

while (($# > 0)); do
  case "$1" in
    --dry-run)
      AGENTD_HUB_DRY_RUN=true
      shift
      ;;
    --binary)
      (($# >= 2)) || agentd_hub_usage
      AGENTD_HUB_BINARY=$2
      shift 2
      ;;
    --output-dir)
      (($# >= 2)) || agentd_hub_usage
      AGENTD_HUB_OUTPUT_DIR=$2
      shift 2
      ;;
    --source-date-epoch)
      (($# >= 2)) || agentd_hub_usage
      AGENTD_HUB_SOURCE_DATE_EPOCH=$2
      shift 2
      ;;
    *)
      agentd_hub_usage
      ;;
  esac
done

[[ -x "$AGENTD_HUB_BINARY" ]] || {
  echo "package release: binary is not executable: $AGENTD_HUB_BINARY" >&2
  exit 1
}

AGENTD_HUB_CARGO_VERSION=$(sed -n 's/^version = "\([^"]*\)"$/\1/p' "$AGENTD_HUB_REPO_ROOT/Cargo.toml")
[[ "$AGENTD_HUB_CARGO_VERSION" == "$AGENTD_HUB_RELEASE_VERSION" ]] || {
  echo "package release: Cargo version is $AGENTD_HUB_CARGO_VERSION, expected $AGENTD_HUB_RELEASE_VERSION" >&2
  exit 1
}

AGENTD_HUB_BINARY_VERSION=$("$AGENTD_HUB_BINARY" --version)
[[ "$AGENTD_HUB_BINARY_VERSION" == "agentd-hub $AGENTD_HUB_RELEASE_VERSION" ]] || {
  echo "package release: binary reports '$AGENTD_HUB_BINARY_VERSION', expected 'agentd-hub $AGENTD_HUB_RELEASE_VERSION'" >&2
  exit 1
}

[[ -f "$AGENTD_HUB_REPO_ROOT/README.md" ]] || {
  echo "package release: required file is missing: README.md" >&2
  exit 1
}

if [[ -z "$AGENTD_HUB_SOURCE_DATE_EPOCH" ]]; then
  AGENTD_HUB_SOURCE_DATE_EPOCH=$(git -C "$AGENTD_HUB_REPO_ROOT" show -s --format=%ct HEAD)
fi
[[ "$AGENTD_HUB_SOURCE_DATE_EPOCH" =~ ^[0-9]+$ ]] || {
  echo "package release: source date epoch must be a nonnegative integer" >&2
  exit 1
}

AGENTD_HUB_RUST_HOST=$(rustc -vV | sed -n 's/^host: //p')
[[ -n "$AGENTD_HUB_RUST_HOST" ]] || {
  echo "package release: rustc did not report a host" >&2
  exit 1
}

AGENTD_HUB_PACKAGE_NAME="agentd-hub-$AGENTD_HUB_RELEASE_VERSION-$AGENTD_HUB_RUST_HOST"
AGENTD_HUB_ARCHIVE_NAME="$AGENTD_HUB_PACKAGE_NAME.tar.gz"

printf 'version=%s\n' "$AGENTD_HUB_RELEASE_VERSION"
printf 'rust_host=%s\n' "$AGENTD_HUB_RUST_HOST"
printf 'source_date_epoch=%s\n' "$AGENTD_HUB_SOURCE_DATE_EPOCH"
printf 'archive=%s\n' "$AGENTD_HUB_ARCHIVE_NAME"
printf 'mode=0755 path=%s/\n' "$AGENTD_HUB_PACKAGE_NAME"
printf 'mode=0755 path=%s/agentd-hub\n' "$AGENTD_HUB_PACKAGE_NAME"
printf 'mode=0644 path=%s/README.md\n' "$AGENTD_HUB_PACKAGE_NAME"

if [[ "$AGENTD_HUB_DRY_RUN" == true ]]; then
  exit 0
fi

mkdir -p "$AGENTD_HUB_REPO_ROOT/target" "$AGENTD_HUB_OUTPUT_DIR"
AGENTD_HUB_STAGE_ROOT=$(mktemp -d "$AGENTD_HUB_REPO_ROOT/target/agentd-hub-package.XXXXXX")
AGENTD_HUB_ARCHIVE_TEMP=$(mktemp "$AGENTD_HUB_OUTPUT_DIR/.${AGENTD_HUB_ARCHIVE_NAME}.XXXXXX")
agentd_hub_cleanup() {
  rm -rf -- "$AGENTD_HUB_STAGE_ROOT"
  rm -f -- "$AGENTD_HUB_ARCHIVE_TEMP"
}
trap agentd_hub_cleanup EXIT

install -Dm755 "$AGENTD_HUB_BINARY" "$AGENTD_HUB_STAGE_ROOT/$AGENTD_HUB_PACKAGE_NAME/agentd-hub"
install -Dm644 "$AGENTD_HUB_REPO_ROOT/README.md" "$AGENTD_HUB_STAGE_ROOT/$AGENTD_HUB_PACKAGE_NAME/README.md"
find "$AGENTD_HUB_STAGE_ROOT/$AGENTD_HUB_PACKAGE_NAME" -type d -exec chmod 0755 {} +

tar \
  --sort=name \
  --format=ustar \
  --mtime="@$AGENTD_HUB_SOURCE_DATE_EPOCH" \
  --owner=0 \
  --group=0 \
  --numeric-owner \
  -cf - \
  -C "$AGENTD_HUB_STAGE_ROOT" \
  "$AGENTD_HUB_PACKAGE_NAME" | gzip -n -9 >"$AGENTD_HUB_ARCHIVE_TEMP"

mv -f -- "$AGENTD_HUB_ARCHIVE_TEMP" "$AGENTD_HUB_OUTPUT_DIR/$AGENTD_HUB_ARCHIVE_NAME"
(
  cd -- "$AGENTD_HUB_OUTPUT_DIR"
  sha256sum "$AGENTD_HUB_ARCHIVE_NAME" >SHA256SUMS
)
printf 'sha256=%s\n' "$(cut -d' ' -f1 "$AGENTD_HUB_OUTPUT_DIR/SHA256SUMS")"
