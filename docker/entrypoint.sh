#!/bin/sh
set -eu

DATA_DIR="${VAYLIX_DATA_DIR:-/var/lib/vaylix}"
BACKUP_DIR="${VAYLIX_BACKUP_DIR:-${DATA_DIR}/backups}"
TARGET_UID="${VAYLIX_RUNTIME_UID:-65532}"
TARGET_GID="${VAYLIX_RUNTIME_GID:-65532}"

mkdir -p "${DATA_DIR}" "${BACKUP_DIR}"

# Bind mounts replace image-owned paths with host-owned inodes. Fix ownership
# before dropping privileges so Linux host mounts work without manual chown.
chown -R "${TARGET_UID}:${TARGET_GID}" "${DATA_DIR}"

exec gosu "${TARGET_UID}:${TARGET_GID}" "$@"
