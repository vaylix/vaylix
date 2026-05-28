use crate::command::{Command, Expiration, SetCondition, SetOptions};
use crate::lexer::{Token, tokenize};
use crate::{CommandError, Result};

/// Parser for the Vaylix command language.
pub struct Parser;

impl Parser {
    /// Parses a single CLI command into its typed representation.
    pub fn parse(input: &str) -> Result<Command> {
        let tokens = tokenize(input)?;

        if tokens.is_empty() {
            return Err(CommandError::EmptyCommand);
        }

        let command = Self::token_text(&tokens[0]).to_ascii_lowercase();

        match command.as_str() {
            "auth" => Self::parse_auth(&tokens),
            "ping" => Self::parse_ping(&tokens),
            "get" => Self::parse_get(&tokens),
            "set" => Self::parse_set(&tokens),
            "setnx" => Self::parse_setnx(&tokens),
            "getdel" => {
                Self::parse_single_key(&tokens, "getdel <key>", |key| Command::GetDel { key })
            }
            "getex" => Self::parse_getex(&tokens),
            "mget" => Self::parse_mget(&tokens),
            "mset" => Self::parse_mset(&tokens),
            "del" | "delete" => Self::parse_delete(&tokens),
            "exists" => Self::parse_exists(&tokens),
            "incr" => Self::parse_single_key(&tokens, "incr <key>", |key| Command::Incr { key }),
            "decr" => Self::parse_single_key(&tokens, "decr <key>", |key| Command::Decr { key }),
            "expire" => Self::parse_expire(&tokens),
            "ttl" => Self::parse_single_key(&tokens, "ttl <key>", |key| Command::Ttl { key }),
            "persist" => {
                Self::parse_single_key(&tokens, "persist <key>", |key| Command::Persist { key })
            }
            "rename" => Self::parse_rename(&tokens, false),
            "renamenx" => Self::parse_rename(&tokens, true),
            "scan" => Self::parse_scan(&tokens),
            "dbsize" => Self::parse_no_args(&tokens, Command::DbSize, "dbsize"),
            "count" => Self::parse_no_args(&tokens, Command::Count, "count"),
            "info" => Self::parse_no_args(&tokens, Command::Info, "info"),
            "metrics" => Self::parse_metrics(&tokens),
            "list" => Self::parse_no_args(&tokens, Command::List, "list"),
            "clear" => Self::parse_no_args(&tokens, Command::Clear, "clear"),
            "flushdb" => Self::parse_no_args(&tokens, Command::Clear, "flushdb"),
            "help" => Self::parse_no_args(&tokens, Command::Help, "help"),
            "exit" | "quit" => Self::parse_no_args(&tokens, Command::Exit, "exit"),
            "save" => Self::parse_no_args(&tokens, Command::Save, "save"),
            "snapshot" => Self::parse_no_args(&tokens, Command::Snapshot, "snapshot"),
            "backup" => Self::parse_backup(&tokens),
            "restore" => Self::parse_restore(&tokens),
            "alter" => Self::parse_alter(&tokens),
            "create" => Self::parse_create(&tokens),
            "drop" => Self::parse_drop(&tokens),
            "grant" => Self::parse_grant(&tokens),
            "revoke" => Self::parse_revoke(&tokens),
            "show" => Self::parse_show(&tokens),
            "whoami" => Self::parse_no_args(&tokens, Command::WhoAmI, "whoami"),
            "multi" => Self::parse_no_args(&tokens, Command::Multi, "multi"),
            "exec" => Self::parse_no_args(&tokens, Command::Exec, "exec"),
            "discard" => Self::parse_no_args(&tokens, Command::Discard, "discard"),
            unknown => Err(CommandError::UnknownCommand {
                command: unknown.to_string(),
            }),
        }
    }

    fn parse_auth(tokens: &[Token]) -> Result<Command> {
        Self::expect_len(tokens, 3, "auth <username> <password>")?;
        Ok(Command::Auth {
            username: Self::token_text(&tokens[1]).to_string(),
            password: Self::token_text(&tokens[2]).to_string(),
        })
    }

    fn parse_ping(tokens: &[Token]) -> Result<Command> {
        match tokens.len() {
            1 => Ok(Command::Ping { message: None }),
            2 => Ok(Command::Ping {
                message: Some(Self::token_text(&tokens[1]).to_string()),
            }),
            _ => Err(CommandError::InvalidArity {
                usage: "ping [message]".to_string(),
            }),
        }
    }

    fn parse_get(tokens: &[Token]) -> Result<Command> {
        Self::expect_len(tokens, 2, "get <key>")?;

        Ok(Command::Get {
            key: Self::token_text(&tokens[1]).to_string(),
        })
    }

    fn parse_set(tokens: &[Token]) -> Result<Command> {
        if tokens.len() < 3 {
            return Err(CommandError::InvalidArity {
                usage: "set <key> <value> [nx|xx] [ex <seconds>|px <millis>] [keepttl] [get]"
                    .to_string(),
            });
        }

        let key = Self::token_text(&tokens[1]).to_string();
        let value = Self::token_text(&tokens[2]).to_string();
        let mut options = SetOptions::default();
        let mut index = 3;

        while index < tokens.len() {
            match Self::token_text(&tokens[index])
                .to_ascii_lowercase()
                .as_str()
            {
                "nx" => {
                    Self::set_condition("set", &mut options, SetCondition::Nx)?;
                    index += 1;
                }
                "xx" => {
                    Self::set_condition("set", &mut options, SetCondition::Xx)?;
                    index += 1;
                }
                "ex" => {
                    let seconds = Self::parse_following_u64(tokens, index, "seconds", "set")?;
                    Self::set_expiration("set", &mut options, Expiration::Ex(seconds))?;
                    index += 2;
                }
                "px" => {
                    let millis = Self::parse_following_u64(tokens, index, "milliseconds", "set")?;
                    Self::set_expiration("set", &mut options, Expiration::Px(millis))?;
                    index += 2;
                }
                "keepttl" => {
                    if options.keep_ttl {
                        return Err(CommandError::ConflictingOptions {
                            command: "set",
                            detail: "KEEPTTL can only be specified once",
                        });
                    }
                    if options.expiration.is_some() {
                        return Err(CommandError::ConflictingOptions {
                            command: "set",
                            detail: "KEEPTTL cannot be combined with EX or PX",
                        });
                    }
                    options.keep_ttl = true;
                    index += 1;
                }
                "get" => {
                    if options.return_previous {
                        return Err(CommandError::ConflictingOptions {
                            command: "set",
                            detail: "GET can only be specified once",
                        });
                    }
                    options.return_previous = true;
                    index += 1;
                }
                other => {
                    return Err(CommandError::InvalidOption {
                        command: "set",
                        option: other.to_string(),
                    });
                }
            }
        }

        Ok(Command::Set {
            key,
            value,
            options,
        })
    }

    fn parse_setnx(tokens: &[Token]) -> Result<Command> {
        Self::expect_len(tokens, 3, "setnx <key> <value>")?;

        Ok(Command::SetNx {
            key: Self::token_text(&tokens[1]).to_string(),
            value: Self::token_text(&tokens[2]).to_string(),
        })
    }

    fn parse_getex(tokens: &[Token]) -> Result<Command> {
        if !(2..=4).contains(&tokens.len()) {
            return Err(CommandError::InvalidArity {
                usage: "getex <key> [ex <seconds>|px <millis>|persist]".to_string(),
            });
        }

        let key = Self::token_text(&tokens[1]).to_string();
        let mut expiration = None;
        let mut persist = false;
        let mut index = 2;

        while index < tokens.len() {
            match Self::token_text(&tokens[index])
                .to_ascii_lowercase()
                .as_str()
            {
                "ex" => {
                    if persist {
                        return Err(CommandError::ConflictingOptions {
                            command: "getex",
                            detail: "PERSIST cannot be combined with EX or PX",
                        });
                    }
                    if expiration.is_some() {
                        return Err(CommandError::ConflictingOptions {
                            command: "getex",
                            detail: "only one expiration modifier is allowed",
                        });
                    }

                    let seconds = Self::parse_following_u64(tokens, index, "seconds", "getex")?;
                    expiration = Some(Expiration::Ex(seconds));
                    index += 2;
                }
                "px" => {
                    if persist {
                        return Err(CommandError::ConflictingOptions {
                            command: "getex",
                            detail: "PERSIST cannot be combined with EX or PX",
                        });
                    }
                    if expiration.is_some() {
                        return Err(CommandError::ConflictingOptions {
                            command: "getex",
                            detail: "only one expiration modifier is allowed",
                        });
                    }

                    let millis = Self::parse_following_u64(tokens, index, "milliseconds", "getex")?;
                    expiration = Some(Expiration::Px(millis));
                    index += 2;
                }
                "persist" => {
                    if persist {
                        return Err(CommandError::ConflictingOptions {
                            command: "getex",
                            detail: "PERSIST can only be specified once",
                        });
                    }
                    if expiration.is_some() {
                        return Err(CommandError::ConflictingOptions {
                            command: "getex",
                            detail: "PERSIST cannot be combined with EX or PX",
                        });
                    }
                    persist = true;
                    index += 1;
                }
                other => {
                    return Err(CommandError::InvalidOption {
                        command: "getex",
                        option: other.to_string(),
                    });
                }
            }
        }

        Ok(Command::GetEx {
            key,
            expiration,
            persist,
        })
    }

    fn parse_mget(tokens: &[Token]) -> Result<Command> {
        if tokens.len() < 2 {
            return Err(CommandError::InvalidArity {
                usage: "mget <key> [key ...]".to_string(),
            });
        }

        Ok(Command::MGet {
            keys: tokens[1..]
                .iter()
                .map(|token| Self::token_text(token).to_string())
                .collect(),
        })
    }

    fn parse_mset(tokens: &[Token]) -> Result<Command> {
        if tokens.len() < 3 || tokens.len().is_multiple_of(2) {
            return Err(CommandError::InvalidArity {
                usage: "mset <key> <value> [key value ...]".to_string(),
            });
        }

        let mut entries = Vec::with_capacity((tokens.len() - 1) / 2);
        let mut index = 1;

        while index < tokens.len() {
            entries.push((
                Self::token_text(&tokens[index]).to_string(),
                Self::token_text(&tokens[index + 1]).to_string(),
            ));
            index += 2;
        }

        Ok(Command::MSet { entries })
    }

    fn parse_delete(tokens: &[Token]) -> Result<Command> {
        if tokens.len() < 2 {
            return Err(CommandError::InvalidArity {
                usage: "del <key> [key ...]".to_string(),
            });
        }

        Ok(Command::Delete {
            keys: tokens[1..]
                .iter()
                .map(|token| Self::token_text(token).to_string())
                .collect(),
        })
    }

    fn parse_exists(tokens: &[Token]) -> Result<Command> {
        Self::expect_len(tokens, 2, "exists <key>")?;

        Ok(Command::Exists {
            key: Self::token_text(&tokens[1]).to_string(),
        })
    }

    fn parse_expire(tokens: &[Token]) -> Result<Command> {
        Self::expect_len(tokens, 3, "expire <key> <seconds>")?;

        Ok(Command::Expire {
            key: Self::token_text(&tokens[1]).to_string(),
            seconds: Self::parse_u64("seconds", &tokens[2])?,
        })
    }

    fn parse_rename(tokens: &[Token], nx: bool) -> Result<Command> {
        let usage = if nx {
            "renamenx <source> <destination>"
        } else {
            "rename <source> <destination>"
        };
        Self::expect_len(tokens, 3, usage)?;
        let source = Self::token_text(&tokens[1]).to_string();
        let destination = Self::token_text(&tokens[2]).to_string();
        Ok(if nx {
            Command::RenameNx {
                source,
                destination,
            }
        } else {
            Command::Rename {
                source,
                destination,
            }
        })
    }

    fn parse_scan(tokens: &[Token]) -> Result<Command> {
        if tokens.len() < 2 {
            return Err(CommandError::InvalidArity {
                usage: "scan <cursor> [match <pattern>] [count <n>]".to_string(),
            });
        }

        let cursor = Self::parse_u64("cursor", &tokens[1])?;
        let mut pattern = None;
        let mut count = None;
        let mut index = 2;

        while index < tokens.len() {
            match Self::token_text(&tokens[index])
                .to_ascii_lowercase()
                .as_str()
            {
                "match" => {
                    if pattern.is_some() {
                        return Err(CommandError::ConflictingOptions {
                            command: "scan",
                            detail: "MATCH can only be specified once",
                        });
                    }
                    if index + 1 >= tokens.len() {
                        return Err(CommandError::InvalidArity {
                            usage: "scan <cursor> [match <pattern>] [count <n>]".to_string(),
                        });
                    }
                    pattern = Some(Self::token_text(&tokens[index + 1]).to_string());
                    index += 2;
                }
                "count" => {
                    if count.is_some() {
                        return Err(CommandError::ConflictingOptions {
                            command: "scan",
                            detail: "COUNT can only be specified once",
                        });
                    }
                    count = Some(Self::parse_following_u16(tokens, index, "count", "scan")?);
                    index += 2;
                }
                other => {
                    return Err(CommandError::InvalidOption {
                        command: "scan",
                        option: other.to_string(),
                    });
                }
            }
        }

        Ok(Command::Scan {
            cursor,
            pattern,
            count,
        })
    }

    fn parse_backup(tokens: &[Token]) -> Result<Command> {
        match tokens.len() {
            1 => Ok(Command::Backup),
            3 if Self::token_text(&tokens[1]).eq_ignore_ascii_case("to") => Ok(Command::BackupTo {
                path: Self::token_text(&tokens[2]).to_string(),
            }),
            3 if Self::token_text(&tokens[1]).eq_ignore_ascii_case("verify")
                && !Self::token_text(&tokens[2]).eq_ignore_ascii_case("from") =>
            {
                Ok(Command::BackupVerify {
                    dump: Self::token_text(&tokens[2]).to_string(),
                })
            }
            4 if Self::token_text(&tokens[1]).eq_ignore_ascii_case("verify")
                && Self::token_text(&tokens[2]).eq_ignore_ascii_case("from") =>
            {
                Ok(Command::BackupVerifyFrom {
                    path: Self::token_text(&tokens[3]).to_string(),
                })
            }
            _ => Err(CommandError::InvalidArity {
                usage: "backup [to <path>] | backup verify <logical-dump-json> | backup verify from <path>".to_string(),
            }),
        }
    }

    fn parse_metrics(tokens: &[Token]) -> Result<Command> {
        match tokens.len() {
            1 => Ok(Command::Metrics),
            2 if Self::token_text(&tokens[1]).eq_ignore_ascii_case("prom") => {
                Ok(Command::MetricsProm)
            }
            _ => Err(CommandError::InvalidArity {
                usage: "metrics [prom]".to_string(),
            }),
        }
    }

    fn parse_restore(tokens: &[Token]) -> Result<Command> {
        match tokens.len() {
            2 => Ok(Command::Restore {
                dump: Self::token_text(&tokens[1]).to_string(),
            }),
            3 if Self::token_text(&tokens[1]).eq_ignore_ascii_case("from") => {
                Ok(Command::RestoreFrom {
                    path: Self::token_text(&tokens[2]).to_string(),
                })
            }
            3 if Self::token_text(&tokens[1]).eq_ignore_ascii_case("check")
                && !Self::token_text(&tokens[2]).eq_ignore_ascii_case("from") =>
            {
                Ok(Command::RestoreCheck {
                    dump: Self::token_text(&tokens[2]).to_string(),
                })
            }
            4 if Self::token_text(&tokens[1]).eq_ignore_ascii_case("check")
                && Self::token_text(&tokens[2]).eq_ignore_ascii_case("from") =>
            {
                Ok(Command::RestoreCheckFrom {
                    path: Self::token_text(&tokens[3]).to_string(),
                })
            }
            _ => Err(CommandError::InvalidArity {
                usage: "restore <logical-dump-json> | restore from <path> | restore check <logical-dump-json> | restore check from <path>".to_string(),
            }),
        }
    }

    fn parse_alter(tokens: &[Token]) -> Result<Command> {
        Self::expect_len(tokens, 5, "alter user <username> password <password>")?;
        if !Self::token_text(&tokens[1]).eq_ignore_ascii_case("user")
            || !Self::token_text(&tokens[3]).eq_ignore_ascii_case("password")
        {
            return Err(CommandError::InvalidArity {
                usage: "alter user <username> password <password>".to_string(),
            });
        }
        Ok(Command::AlterUserPassword {
            username: Self::token_text(&tokens[2]).to_string(),
            password: Self::token_text(&tokens[4]).to_string(),
        })
    }

    fn parse_create(tokens: &[Token]) -> Result<Command> {
        if tokens.len() < 3 {
            return Err(CommandError::InvalidArity {
                usage: "create user <username> password <password> | create role <role>"
                    .to_string(),
            });
        }

        match Self::token_text(&tokens[1]).to_ascii_lowercase().as_str() {
            "user" => {
                Self::expect_len(tokens, 5, "create user <username> password <password>")?;
                if !Self::token_text(&tokens[3]).eq_ignore_ascii_case("password") {
                    return Err(CommandError::InvalidArity {
                        usage: "create user <username> password <password>".to_string(),
                    });
                }
                Ok(Command::CreateUser {
                    username: Self::token_text(&tokens[2]).to_string(),
                    password: Self::token_text(&tokens[4]).to_string(),
                })
            }
            "role" => {
                Self::expect_len(tokens, 3, "create role <role>")?;
                Ok(Command::CreateRole {
                    role: Self::token_text(&tokens[2]).to_string(),
                })
            }
            other => Err(CommandError::InvalidOption {
                command: "create",
                option: other.to_string(),
            }),
        }
    }

    fn parse_drop(tokens: &[Token]) -> Result<Command> {
        if tokens.len() < 3 {
            return Err(CommandError::InvalidArity {
                usage: "drop user <username> | drop role <role>".to_string(),
            });
        }

        match Self::token_text(&tokens[1]).to_ascii_lowercase().as_str() {
            "user" => {
                Self::expect_len(tokens, 3, "drop user <username>")?;
                Ok(Command::DropUser {
                    username: Self::token_text(&tokens[2]).to_string(),
                })
            }
            "role" => {
                Self::expect_len(tokens, 3, "drop role <role>")?;
                Ok(Command::DropRole {
                    role: Self::token_text(&tokens[2]).to_string(),
                })
            }
            other => Err(CommandError::InvalidOption {
                command: "drop",
                option: other.to_string(),
            }),
        }
    }

    fn parse_grant(tokens: &[Token]) -> Result<Command> {
        if tokens.len() != 5 && tokens.len() != 7 {
            return Err(CommandError::InvalidArity {
                usage: "grant role <role> to <username> | grant permission <permission> [on <pattern>] to <role>".to_string(),
            });
        }

        match Self::token_text(&tokens[1]).to_ascii_lowercase().as_str() {
            "role"
                if tokens.len() == 5 && Self::token_text(&tokens[3]).eq_ignore_ascii_case("to") =>
            {
                Ok(Command::GrantRole {
                    role: Self::token_text(&tokens[2]).to_string(),
                    username: Self::token_text(&tokens[4]).to_string(),
                })
            }
            "permission"
                if tokens.len() == 5 && Self::token_text(&tokens[3]).eq_ignore_ascii_case("to") =>
            {
                Ok(Command::GrantPermission {
                    permission: Self::token_text(&tokens[2]).to_string(),
                    pattern: "*".to_string(),
                    role: Self::token_text(&tokens[4]).to_string(),
                })
            }
            "permission"
                if tokens.len() == 7
                    && Self::token_text(&tokens[3]).eq_ignore_ascii_case("on")
                    && Self::token_text(&tokens[5]).eq_ignore_ascii_case("to") =>
            {
                Ok(Command::GrantPermission {
                    permission: Self::token_text(&tokens[2]).to_string(),
                    pattern: Self::token_text(&tokens[4]).to_string(),
                    role: Self::token_text(&tokens[6]).to_string(),
                })
            }
            other => Err(CommandError::InvalidOption {
                command: "grant",
                option: other.to_string(),
            }),
        }
    }

    fn parse_revoke(tokens: &[Token]) -> Result<Command> {
        if tokens.len() != 5 && tokens.len() != 7 {
            return Err(CommandError::InvalidArity {
                usage:
                    "revoke role <role> from <username> | revoke permission <permission> [on <pattern>] from <role>"
                        .to_string(),
            });
        }

        match Self::token_text(&tokens[1]).to_ascii_lowercase().as_str() {
            "role"
                if tokens.len() == 5
                    && Self::token_text(&tokens[3]).eq_ignore_ascii_case("from") =>
            {
                Ok(Command::RevokeRole {
                    role: Self::token_text(&tokens[2]).to_string(),
                    username: Self::token_text(&tokens[4]).to_string(),
                })
            }
            "permission"
                if tokens.len() == 5
                    && Self::token_text(&tokens[3]).eq_ignore_ascii_case("from") =>
            {
                Ok(Command::RevokePermission {
                    permission: Self::token_text(&tokens[2]).to_string(),
                    pattern: "*".to_string(),
                    role: Self::token_text(&tokens[4]).to_string(),
                })
            }
            "permission"
                if tokens.len() == 7
                    && Self::token_text(&tokens[3]).eq_ignore_ascii_case("on")
                    && Self::token_text(&tokens[5]).eq_ignore_ascii_case("from") =>
            {
                Ok(Command::RevokePermission {
                    permission: Self::token_text(&tokens[2]).to_string(),
                    pattern: Self::token_text(&tokens[4]).to_string(),
                    role: Self::token_text(&tokens[6]).to_string(),
                })
            }
            other => Err(CommandError::InvalidOption {
                command: "revoke",
                option: other.to_string(),
            }),
        }
    }

    fn parse_show(tokens: &[Token]) -> Result<Command> {
        if tokens.len() != 2 && tokens.len() != 5 {
            return Err(CommandError::InvalidArity {
                usage: "show users | show roles | show grants | show grants for user <username> | show grants for role <role>"
                    .to_string(),
            });
        }
        match Self::token_text(&tokens[1]).to_ascii_lowercase().as_str() {
            "users" if tokens.len() == 2 => Ok(Command::ShowUsers),
            "roles" if tokens.len() == 2 => Ok(Command::ShowRoles),
            "grants" if tokens.len() == 2 => Ok(Command::ShowGrants),
            "grants"
                if tokens.len() == 5
                    && Self::token_text(&tokens[2]).eq_ignore_ascii_case("for")
                    && Self::token_text(&tokens[3]).eq_ignore_ascii_case("user") =>
            {
                Ok(Command::ShowGrantsForUser {
                    username: Self::token_text(&tokens[4]).to_string(),
                })
            }
            "grants"
                if tokens.len() == 5
                    && Self::token_text(&tokens[2]).eq_ignore_ascii_case("for")
                    && Self::token_text(&tokens[3]).eq_ignore_ascii_case("role") =>
            {
                Ok(Command::ShowGrantsForRole {
                    role: Self::token_text(&tokens[4]).to_string(),
                })
            }
            other => Err(CommandError::InvalidOption {
                command: "show",
                option: other.to_string(),
            }),
        }
    }

    fn parse_single_key<F>(tokens: &[Token], usage: &str, constructor: F) -> Result<Command>
    where
        F: FnOnce(String) -> Command,
    {
        Self::expect_len(tokens, 2, usage)?;
        Ok(constructor(Self::token_text(&tokens[1]).to_string()))
    }

    fn parse_no_args(tokens: &[Token], command: Command, usage: &str) -> Result<Command> {
        if tokens.len() != 1 {
            return Err(CommandError::InvalidArity {
                usage: usage.to_string(),
            });
        }

        Ok(command)
    }

    fn expect_len(tokens: &[Token], expected: usize, usage: &str) -> Result<()> {
        if tokens.len() != expected {
            return Err(CommandError::InvalidArity {
                usage: usage.to_string(),
            });
        }

        Ok(())
    }

    fn parse_u64(field: &'static str, token: &Token) -> Result<u64> {
        Self::token_text(token)
            .parse()
            .map_err(|_| CommandError::InvalidInteger {
                field,
                value: Self::token_text(token).to_string(),
            })
    }

    fn parse_u16(field: &'static str, token: &Token) -> Result<u16> {
        Self::token_text(token)
            .parse()
            .map_err(|_| CommandError::InvalidInteger {
                field,
                value: Self::token_text(token).to_string(),
            })
    }

    fn parse_following_u64(
        tokens: &[Token],
        index: usize,
        field: &'static str,
        usage_command: &str,
    ) -> Result<u64> {
        let Some(token) = tokens.get(index + 1) else {
            return Err(CommandError::InvalidArity {
                usage: match usage_command {
                    "set" => "set <key> <value> [nx|xx] [ex <seconds>|px <millis>] [keepttl] [get]",
                    "getex" => "getex <key> [ex <seconds>|px <millis>|persist]",
                    _ => usage_command,
                }
                .to_string(),
            });
        };

        Self::parse_u64(field, token)
    }

    fn parse_following_u16(
        tokens: &[Token],
        index: usize,
        field: &'static str,
        usage_command: &str,
    ) -> Result<u16> {
        let Some(token) = tokens.get(index + 1) else {
            return Err(CommandError::InvalidArity {
                usage: match usage_command {
                    "scan" => "scan <cursor> [match <pattern>] [count <n>]",
                    _ => usage_command,
                }
                .to_string(),
            });
        };

        Self::parse_u16(field, token)
    }

    fn set_condition(
        command: &'static str,
        options: &mut SetOptions,
        condition: SetCondition,
    ) -> Result<()> {
        if options.condition.is_some() {
            return Err(CommandError::ConflictingOptions {
                command,
                detail: "only one of NX or XX may be specified",
            });
        }

        options.condition = Some(condition);
        Ok(())
    }

    fn set_expiration(
        command: &'static str,
        options: &mut SetOptions,
        expiration: Expiration,
    ) -> Result<()> {
        if options.keep_ttl {
            return Err(CommandError::ConflictingOptions {
                command,
                detail: "EX or PX cannot be combined with KEEPTTL",
            });
        }
        if options.expiration.is_some() {
            return Err(CommandError::ConflictingOptions {
                command,
                detail: "only one of EX or PX may be specified",
            });
        }

        options.expiration = Some(expiration);
        Ok(())
    }

    fn token_text(token: &Token) -> &str {
        match token {
            Token::Word(value) | Token::QuotedString(value) => value,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Parser;
    use crate::{Command, Expiration, SetCondition, SetOptions};

    #[test]
    fn parses_serious_v1_commands() {
        assert_eq!(
            Parser::parse("ping").unwrap(),
            Command::Ping { message: None }
        );
        assert_eq!(
            Parser::parse("ping pong").unwrap(),
            Command::Ping {
                message: Some("pong".to_string())
            }
        );
        assert_eq!(
            Parser::parse("mget one two").unwrap(),
            Command::MGet {
                keys: vec!["one".to_string(), "two".to_string()]
            }
        );
        assert_eq!(
            Parser::parse("mset one 1 two 2").unwrap(),
            Command::MSet {
                entries: vec![
                    ("one".to_string(), "1".to_string()),
                    ("two".to_string(), "2".to_string())
                ]
            }
        );
        assert_eq!(
            Parser::parse("setnx name alice").unwrap(),
            Command::SetNx {
                key: "name".to_string(),
                value: "alice".to_string()
            }
        );
        assert_eq!(
            Parser::parse("getdel token").unwrap(),
            Command::GetDel {
                key: "token".to_string()
            }
        );
        assert_eq!(
            Parser::parse("getex token ex 60").unwrap(),
            Command::GetEx {
                key: "token".to_string(),
                expiration: Some(Expiration::Ex(60)),
                persist: false,
            }
        );
        assert_eq!(
            Parser::parse("expire token 60").unwrap(),
            Command::Expire {
                key: "token".to_string(),
                seconds: 60
            }
        );
        assert_eq!(
            Parser::parse("ttl token").unwrap(),
            Command::Ttl {
                key: "token".to_string()
            }
        );
        assert_eq!(
            Parser::parse("persist token").unwrap(),
            Command::Persist {
                key: "token".to_string()
            }
        );
        assert_eq!(
            Parser::parse("scan 0 match user:* count 25").unwrap(),
            Command::Scan {
                cursor: 0,
                pattern: Some("user:*".to_string()),
                count: Some(25)
            }
        );
        assert_eq!(Parser::parse("dbsize").unwrap(), Command::DbSize);
        assert_eq!(Parser::parse("info").unwrap(), Command::Info);
        assert_eq!(Parser::parse("metrics").unwrap(), Command::Metrics);
        assert_eq!(Parser::parse("metrics prom").unwrap(), Command::MetricsProm);
        assert_eq!(Parser::parse("save").unwrap(), Command::Save);
        assert_eq!(Parser::parse("backup").unwrap(), Command::Backup);
        assert_eq!(
            Parser::parse("backup to nightly.json").unwrap(),
            Command::BackupTo {
                path: "nightly.json".to_string()
            }
        );
        assert_eq!(
            Parser::parse(r#"backup verify "{\"version\":1}""#).unwrap(),
            Command::BackupVerify {
                dump: "{\"version\":1}".to_string()
            }
        );
        assert_eq!(
            Parser::parse("backup verify from nightly.json").unwrap(),
            Command::BackupVerifyFrom {
                path: "nightly.json".to_string()
            }
        );
        assert_eq!(
            Parser::parse(r#"restore "{\"version\":1}""#).unwrap(),
            Command::Restore {
                dump: "{\"version\":1}".to_string()
            }
        );
        assert_eq!(
            Parser::parse("restore from nightly.json").unwrap(),
            Command::RestoreFrom {
                path: "nightly.json".to_string()
            }
        );
        assert_eq!(
            Parser::parse(r#"restore check "{\"version\":1}""#).unwrap(),
            Command::RestoreCheck {
                dump: "{\"version\":1}".to_string()
            }
        );
        assert_eq!(
            Parser::parse("restore check from nightly.json").unwrap(),
            Command::RestoreCheckFrom {
                path: "nightly.json".to_string()
            }
        );
        assert_eq!(
            Parser::parse("del key").unwrap(),
            Command::Delete {
                keys: vec!["key".to_string()]
            }
        );
        assert_eq!(
            Parser::parse("create user alice password secret").unwrap(),
            Command::CreateUser {
                username: "alice".to_string(),
                password: "secret".to_string()
            }
        );
        assert_eq!(
            Parser::parse("alter user alice password new-secret").unwrap(),
            Command::AlterUserPassword {
                username: "alice".to_string(),
                password: "new-secret".to_string()
            }
        );
        assert_eq!(
            Parser::parse("create role readonly").unwrap(),
            Command::CreateRole {
                role: "readonly".to_string()
            }
        );
        assert_eq!(
            Parser::parse("grant role readonly to alice").unwrap(),
            Command::GrantRole {
                role: "readonly".to_string(),
                username: "alice".to_string()
            }
        );
        assert_eq!(
            Parser::parse("grant permission read to readonly").unwrap(),
            Command::GrantPermission {
                permission: "read".to_string(),
                pattern: "*".to_string(),
                role: "readonly".to_string()
            }
        );
        assert_eq!(
            Parser::parse("grant permission read on app:* to readonly").unwrap(),
            Command::GrantPermission {
                permission: "read".to_string(),
                pattern: "app:*".to_string(),
                role: "readonly".to_string()
            }
        );
        assert_eq!(
            Parser::parse("revoke role readonly from alice").unwrap(),
            Command::RevokeRole {
                role: "readonly".to_string(),
                username: "alice".to_string()
            }
        );
        assert_eq!(
            Parser::parse("revoke permission read from readonly").unwrap(),
            Command::RevokePermission {
                permission: "read".to_string(),
                pattern: "*".to_string(),
                role: "readonly".to_string()
            }
        );
        assert_eq!(
            Parser::parse("revoke permission read on app:* from readonly").unwrap(),
            Command::RevokePermission {
                permission: "read".to_string(),
                pattern: "app:*".to_string(),
                role: "readonly".to_string()
            }
        );
        assert_eq!(Parser::parse("show users").unwrap(), Command::ShowUsers);
        assert_eq!(Parser::parse("show roles").unwrap(), Command::ShowRoles);
        assert_eq!(Parser::parse("show grants").unwrap(), Command::ShowGrants);
        assert_eq!(
            Parser::parse("show grants for user alice").unwrap(),
            Command::ShowGrantsForUser {
                username: "alice".to_string()
            }
        );
        assert_eq!(
            Parser::parse("show grants for role readonly").unwrap(),
            Command::ShowGrantsForRole {
                role: "readonly".to_string()
            }
        );
        assert_eq!(Parser::parse("whoami").unwrap(), Command::WhoAmI);
    }

    #[test]
    fn parses_quoted_values() {
        assert_eq!(
            Parser::parse(r#"set name "John Doe""#).unwrap(),
            Command::Set {
                key: "name".to_string(),
                value: "John Doe".to_string(),
                options: SetOptions::default(),
            }
        );
    }

    #[test]
    fn parses_quit_alias() {
        assert_eq!(Parser::parse("quit").unwrap(), Command::Exit);
    }

    #[test]
    fn rejects_invalid_arity_and_numbers() {
        assert!(Parser::parse("get").is_err());
        assert!(Parser::parse("mset name").is_err());
        assert!(Parser::parse("scan").is_err());
        assert!(Parser::parse("expire key nope").is_err());
        assert!(Parser::parse("scan nope").is_err());
        assert!(Parser::parse("set key value ex").is_err());
        assert!(Parser::parse("getex key ex").is_err());
        assert!(Parser::parse("create user alice").is_err());
        assert!(Parser::parse("alter user alice").is_err());
        assert!(Parser::parse("backup to").is_err());
        assert!(Parser::parse("backup verify from").is_err());
        assert!(Parser::parse("backup verify").is_err());
        assert!(Parser::parse("metrics prom extra").is_err());
        assert!(Parser::parse("restore check from").is_err());
        assert!(Parser::parse("grant role readonly alice").is_err());
        assert!(Parser::parse("grant permission read on to readonly").is_err());
        assert!(Parser::parse("grant permission read app:* to readonly").is_err());
        assert!(Parser::parse("revoke permission read on from readonly").is_err());
        assert!(Parser::parse("revoke permission read app:* from readonly").is_err());
        assert!(Parser::parse("show grants for user").is_err());
        assert!(Parser::parse("show grants user alice").is_err());
    }

    #[test]
    fn rejects_unknown_commands_and_empty_input() {
        assert!(Parser::parse("").is_err());
        assert!(Parser::parse("   ").is_err());
        assert!(Parser::parse("unknown thing").is_err());
    }

    #[test]
    fn preserves_case_in_arguments() {
        assert_eq!(
            Parser::parse(r#"set UserName "Alice Smith""#).unwrap(),
            Command::Set {
                key: "UserName".to_string(),
                value: "Alice Smith".to_string(),
                options: SetOptions::default(),
            }
        );
    }

    #[test]
    fn parses_set_options_and_rejects_conflicts() {
        assert_eq!(
            Parser::parse("set cache item nx px 1500 get").unwrap(),
            Command::Set {
                key: "cache".to_string(),
                value: "item".to_string(),
                options: SetOptions {
                    condition: Some(SetCondition::Nx),
                    expiration: Some(Expiration::Px(1500)),
                    keep_ttl: false,
                    return_previous: true,
                },
            }
        );

        assert!(Parser::parse("set key value ex 10 keepttl").is_err());
        assert!(Parser::parse("set key value nx xx").is_err());
        assert!(Parser::parse("scan 0 count 2 count 3").is_err());
    }
}
