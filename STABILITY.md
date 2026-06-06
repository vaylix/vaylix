# Vaylix Stability Policy

Vaylix treats stability as an implementation contract, not a marketing claim.

## Scope

The stability policy covers:

- the VTP protocol negotiation boundary
- command syntax and deterministic error codes
- WAL, snapshot, manifest, keyring, and logical backup formats
- INFO and METRICS field naming
- Docker runtime defaults and data-directory behavior
- documented HA and consistency semantics

## Pre-1.0 Rules

- Patch releases may fix correctness, security, durability, and operational defects.
- Minor releases may add compatible fields, metrics, or diagnostics.
- Storage or protocol incompatibilities must be explicit in release notes.
- Silent storage upgrades are not allowed.
- Implicit migration is not allowed.

## 1.0 Lockdown Target

Before `1.0.0`, Vaylix must have:

- a frozen storage format version
- explicit protocol compatibility rules
- documented consistency semantics
- reproducible release builds
- deterministic recovery behavior for corruption and interrupted writes
- stability documents updated with every compatibility-impacting change

## Error Codes

Error codes are operator-facing API. A code may be added in a minor release, but an existing code must not be reused for a different class of failure. The canonical code list is [ERROR_CODES.md](ERROR_CODES.md).

## INFO and METRICS

Existing field names should remain stable within a major line. New fields may be added. Removing or changing the meaning of an existing field requires a documented compatibility note.
