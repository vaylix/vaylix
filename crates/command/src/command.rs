/// Metadata used by the client REPL for completion and inline help.
#[derive(Debug, Clone, Copy)]
pub struct CommandInfo {
    pub name: &'static str,
    pub usage: &'static str,
}

/// Conditional write behavior for `SET`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetCondition {
    /// Only write when the key does not exist.
    Nx,
    /// Only write when the key already exists.
    Xx,
}

/// Expiration policy expressed in seconds or milliseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Expiration {
    /// Expiration in whole seconds.
    Ex(u64),
    /// Expiration in milliseconds.
    Px(u64),
}

/// Extended modifiers supported by the `SET` command.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SetOptions {
    /// Conditional write requirement.
    pub condition: Option<SetCondition>,
    /// TTL to attach to the value.
    pub expiration: Option<Expiration>,
    /// Preserve the current TTL on overwrite.
    pub keep_ttl: bool,
    /// Return the previous value instead of a plain status.
    pub return_previous: bool,
}

pub const COMMANDS: &[CommandInfo] = &[
    CommandInfo {
        name: "auth",
        usage: "auth <username> <password>",
    },
    CommandInfo {
        name: "ping",
        usage: "ping [message]",
    },
    CommandInfo {
        name: "get",
        usage: "get <key>",
    },
    CommandInfo {
        name: "set",
        usage: "set <key> <value> [nx|xx] [ex <seconds>|px <millis>] [keepttl] [get]",
    },
    CommandInfo {
        name: "setnx",
        usage: "setnx <key> <value>",
    },
    CommandInfo {
        name: "getdel",
        usage: "getdel <key>",
    },
    CommandInfo {
        name: "getex",
        usage: "getex <key> [ex <seconds>|px <millis>|persist]",
    },
    CommandInfo {
        name: "mget",
        usage: "mget <key> [key ...]",
    },
    CommandInfo {
        name: "mset",
        usage: "mset <key> <value> [key value ...]",
    },
    CommandInfo {
        name: "del",
        usage: "del <key> [key ...]",
    },
    CommandInfo {
        name: "delete",
        usage: "delete <key> [key ...]",
    },
    CommandInfo {
        name: "exists",
        usage: "exists <key>",
    },
    CommandInfo {
        name: "incr",
        usage: "incr <key>",
    },
    CommandInfo {
        name: "decr",
        usage: "decr <key>",
    },
    CommandInfo {
        name: "expire",
        usage: "expire <key> <seconds>",
    },
    CommandInfo {
        name: "ttl",
        usage: "ttl <key>",
    },
    CommandInfo {
        name: "persist",
        usage: "persist <key>",
    },
    CommandInfo {
        name: "rename",
        usage: "rename <source> <destination>",
    },
    CommandInfo {
        name: "renamenx",
        usage: "renamenx <source> <destination>",
    },
    CommandInfo {
        name: "scan",
        usage: "scan <cursor> [match <pattern>] [count <n>]",
    },
    CommandInfo {
        name: "dbsize",
        usage: "dbsize",
    },
    CommandInfo {
        name: "count",
        usage: "count",
    },
    CommandInfo {
        name: "info",
        usage: "info",
    },
    CommandInfo {
        name: "metrics",
        usage: "metrics",
    },
    CommandInfo {
        name: "list",
        usage: "list",
    },
    CommandInfo {
        name: "clear",
        usage: "clear",
    },
    CommandInfo {
        name: "flushdb",
        usage: "flushdb",
    },
    CommandInfo {
        name: "save",
        usage: "save",
    },
    CommandInfo {
        name: "snapshot",
        usage: "snapshot",
    },
    CommandInfo {
        name: "backup",
        usage: "backup [to <path>]",
    },
    CommandInfo {
        name: "restore",
        usage: "restore <logical-dump-json> | restore from <path> | restore check <logical-dump-json> | restore check from <path>",
    },
    CommandInfo {
        name: "alter-user",
        usage: "alter user <username> password <password>",
    },
    CommandInfo {
        name: "create-user",
        usage: "create user <username> password <password>",
    },
    CommandInfo {
        name: "drop-user",
        usage: "drop user <username>",
    },
    CommandInfo {
        name: "create-role",
        usage: "create role <role>",
    },
    CommandInfo {
        name: "drop-role",
        usage: "drop role <role>",
    },
    CommandInfo {
        name: "grant-role",
        usage: "grant role <role> to <username>",
    },
    CommandInfo {
        name: "revoke-role",
        usage: "revoke role <role> from <username>",
    },
    CommandInfo {
        name: "grant-permission",
        usage: "grant permission <permission> [on <pattern>] to <role>",
    },
    CommandInfo {
        name: "revoke-permission",
        usage: "revoke permission <permission> [on <pattern>] from <role>",
    },
    CommandInfo {
        name: "show-users",
        usage: "show users",
    },
    CommandInfo {
        name: "show-roles",
        usage: "show roles",
    },
    CommandInfo {
        name: "whoami",
        usage: "whoami",
    },
    CommandInfo {
        name: "multi",
        usage: "multi",
    },
    CommandInfo {
        name: "exec",
        usage: "exec",
    },
    CommandInfo {
        name: "discard",
        usage: "discard",
    },
    CommandInfo {
        name: "help",
        usage: "help",
    },
    CommandInfo {
        name: "exit",
        usage: "exit",
    },
];

pub fn command_info(name: &str) -> Option<&'static CommandInfo> {
    COMMANDS
        .iter()
        .find(|command| command.name.eq_ignore_ascii_case(name))
}

/// Parsed client command independent of transport framing and engine internals.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Auth {
        username: String,
        password: String,
    },
    Ping {
        message: Option<String>,
    },
    Get {
        key: String,
    },
    Set {
        key: String,
        value: String,
        options: SetOptions,
    },
    SetNx {
        key: String,
        value: String,
    },
    GetDel {
        key: String,
    },
    GetEx {
        key: String,
        expiration: Option<Expiration>,
        persist: bool,
    },
    MGet {
        keys: Vec<String>,
    },
    MSet {
        entries: Vec<(String, String)>,
    },
    Delete {
        keys: Vec<String>,
    },
    Exists {
        key: String,
    },
    Incr {
        key: String,
    },
    Decr {
        key: String,
    },
    Expire {
        key: String,
        seconds: u64,
    },
    Ttl {
        key: String,
    },
    Persist {
        key: String,
    },
    Rename {
        source: String,
        destination: String,
    },
    RenameNx {
        source: String,
        destination: String,
    },
    Scan {
        cursor: u64,
        pattern: Option<String>,
        count: Option<u16>,
    },
    DbSize,
    Info,
    Metrics,
    List,
    Clear,
    Count,
    Save,
    Backup,
    BackupTo {
        path: String,
    },
    Restore {
        dump: String,
    },
    RestoreFrom {
        path: String,
    },
    RestoreCheck {
        dump: String,
    },
    RestoreCheckFrom {
        path: String,
    },
    AlterUserPassword {
        username: String,
        password: String,
    },
    CreateUser {
        username: String,
        password: String,
    },
    DropUser {
        username: String,
    },
    CreateRole {
        role: String,
    },
    DropRole {
        role: String,
    },
    GrantRole {
        role: String,
        username: String,
    },
    RevokeRole {
        role: String,
        username: String,
    },
    GrantPermission {
        permission: String,
        pattern: String,
        role: String,
    },
    RevokePermission {
        permission: String,
        pattern: String,
        role: String,
    },
    ShowUsers,
    ShowRoles,
    WhoAmI,
    Multi,
    Exec,
    Discard,
    Help,
    Exit,
    Snapshot,
}
