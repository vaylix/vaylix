# Vaylix Error Code Contract

Error codes are stable client-facing identifiers. A code must not be reused for a different failure class within a major release line. Error names may clarify wording, but the client-observable meaning of an existing code must remain stable.

## Command Parser

| Code | Name | Client meaning |
| --- | --- | --- |
| `CMD-001` | Empty Command | The command parser received no command token. |
| `CMD-002` | Unknown Command | The command token is not supported by this server. |
| `CMD-003` | Invalid Command Arity | The command has the wrong number of arguments. |
| `CMD-004` | Invalid Command Integer | An integer argument could not be parsed. |
| `CMD-005` | Invalid Command Option | An option token is not valid for the command. |
| `CMD-006` | Conflicting Command Options | The command includes mutually exclusive options. |
| `CMD-007` | Expected Opening Quote | Command text expected the start of a quoted string. |
| `CMD-008` | Unterminated Quoted String | Command text ended before a quoted string closed. |

## Engine and Storage

| Code | Name | Client meaning |
| --- | --- | --- |
| `ENG-001` | Project Directories Unavailable | The server could not determine required project directories. |
| `ENG-002` | Filesystem I/O Failure | A filesystem operation failed. |
| `ENG-003` | Snapshot Serialization Failure | Snapshot creation failed during serialization. |
| `ENG-004` | Snapshot Deserialization Failure | Snapshot loading failed during deserialization. |
| `ENG-005` | Manifest Serialization Failure | Manifest creation failed during serialization. |
| `ENG-006` | Manifest Deserialization Failure | Manifest loading failed during deserialization. |
| `ENG-007` | Checksum Validation Failure | A persisted checksum did not match the decoded bytes. |
| `ENG-008` | Encrypted Storage Failure | Encryption or decryption failed for persisted storage. |
| `ENG-009` | Unsupported Storage Format | A storage format version is not supported by this binary. |
| `ENG-010` | Storage Migration Required | Storage cannot be used without an explicit migration. |
| `ENG-011` | Invalid Storage Operation | The requested storage operation is invalid for the current state. |
| `ENG-012` | Restore Point Unavailable | The requested recovery point cannot be found. |
| `ENG-013` | WAL Serialization Failure | WAL entry serialization failed. |
| `ENG-014` | WAL Deserialization Failure | WAL entry deserialization failed. |
| `ENG-015` | Invalid Integer Value | A stored value could not be interpreted as an integer. |
| `ENG-016` | Numeric Overflow | A numeric operation would overflow. |
| `ENG-017` | Unsupported Command | The engine does not support the command variant. |

## Transport

| Code | Name | Client meaning |
| --- | --- | --- |
| `TRN-001` | Invalid Frame | The frame header or layout is invalid. |
| `TRN-002` | Unknown Opcode | The opcode byte is not recognized. |
| `TRN-003` | Unknown Status | The response status byte is not recognized. |
| `TRN-004` | Unsupported Command | The command cannot be encoded on this transport. |
| `TRN-005` | Version Mismatch | Protocol versions are incompatible. |
| `TRN-006` | Frame Too Large | Frame length exceeds the negotiated maximum. |
| `TRN-007` | Unexpected End Of Frame | The frame ended before all declared bytes were available. |
| `TRN-008` | Corrupted Payload | Payload bytes cannot be decoded as the expected structure. |
| `TRN-009` | Checksum Mismatch | Frame checksum validation failed before payload processing. |
| `TRN-010` | Unsupported Frame Flags | Frame flags include unsupported bits. |
| `TRN-011` | Compression Failure | Compression or decompression failed. |
| `TRN-012` | Invalid UTF-8 Payload | A text payload is not valid UTF-8. |
| `TRN-013` | Transport I/O Failure | Socket or stream I/O failed. |
| `TRN-014` | Protocol Negotiation Failed | Startup negotiation was rejected. |
| `TRN-015` | Transport Capability Mismatch | Negotiated capabilities are incompatible. |
| `TRN-016` | Request Deadline Exceeded | Request metadata deadline expired before execution. |
| `TRN-017` | Decompressed Frame Too Large | Decompressed payload exceeds the negotiated maximum. |
| `TRN-018` | Protocol State Violation | The peer violated the expected protocol sequence. |

## Server

| Code | Name | Client meaning |
| --- | --- | --- |
| `SRV-001` | Listener Bind Failure | The server could not bind its configured listener. |
| `SRV-002` | Connection Accept Failure | Accepting a client connection failed. |
| `SRV-003` | Connection Slot Pool Closed | The connection limiter is closed. |
| `SRV-004` | Filesystem I/O Failure | A server filesystem operation failed. |
| `SRV-005` | Engine Worker Closed | The engine worker is unavailable. |
| `SRV-006` | Engine Lock Poisoned | Internal engine lock state is poisoned. |
| `SRV-007` | Authentication Required | The command requires an authenticated session. |
| `SRV-008` | Authentication Failed | Provided credentials are invalid. |
| `SRV-009` | Authentication Configuration Invalid | Server auth configuration is invalid. |
| `SRV-010` | TLS Configuration Invalid | TLS configuration is invalid. |
| `SRV-011` | TLS Handshake Failure | TLS handshake failed. |
| `SRV-012` | Rate Limit Exceeded | Request rate limit rejected the command. |
| `SRV-013` | Quota Exceeded | Request payload, key, value, or batch quota was exceeded. |
| `SRV-014` | Transaction Already Active | The session already has an active transaction. |
| `SRV-015` | No Active Transaction | The session has no transaction to complete or discard. |
| `SRV-016` | Unsupported Remote Command | Remote command execution is not supported for this command. |
| `SRV-017` | Permission Denied | The authenticated identity lacks the required permission. |
| `SRV-018` | Invalid Permission | A permission name is unknown. |
| `SRV-019` | User Already Exists | User creation targets an existing user. |
| `SRV-020` | User Not Found | User mutation targets an unknown user. |
| `SRV-021` | Role Already Exists | Role creation targets an existing role. |
| `SRV-022` | Role Not Found | Role mutation targets an unknown role. |
| `SRV-023` | Protected Role | The role is protected from the requested mutation. |
| `SRV-024` | Last Admin User | The mutation would remove the last administrator. |
| `SRV-025` | Auth Store Serialization Failure | Auth store persistence failed during serialization. |
| `SRV-026` | Auth Store Deserialization Failure | Auth store loading failed during deserialization. |
| `SRV-027` | Backup Path Rejected | Backup or restore path escaped the configured backup directory. |
| `SRV-028` | Audit Chain Verification Failure | Audit log hash-chain verification failed. |
| `SRV-029` | Backup Verification Failure | Backup dump or manifest verification failed. |
| `SRV-030` | Invalid Arguments | Server CLI or command arguments are invalid. |
| `SRV-031` | Authentication Locked | Authentication is temporarily locked for the identity scope. |
| `SRV-032` | Password Policy Violation | Password does not satisfy server policy. |
| `SRV-033` | Maintenance Mode Enabled | The command is blocked by maintenance mode. |
| `SRV-034` | Transaction Expired | Transaction lifetime exceeded the configured limit. |
| `SRV-035` | Replication Acknowledgement Timeout | Write acknowledgement did not complete before timeout. |
| `SRV-036` | Replication Acknowledgement Unavailable | The configured replication acknowledgement cannot currently be satisfied. |
| `SRV-037` | Follower Write Rejected | A follower rejected a write command. |
| `SRV-038` | Replication Promotion Denied | Manual promotion was rejected by safety checks. |
| `SRV-039` | Healthcheck Failed | The server healthcheck command failed. |
