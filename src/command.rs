#[derive(Debug, Clone, Copy)]
pub struct CommandInfo {
    pub name: &'static str,
    pub usage: &'static str,
}

pub const COMMANDS: &[CommandInfo] = &[
    CommandInfo {
        name: "get",
        usage: "get <key>",
    },
    CommandInfo {
        name: "set",
        usage: "set <key> <value>",
    },
    CommandInfo {
        name: "delete",
        usage: "delete <key> [key...]",
    },
    CommandInfo {
        name: "exists",
        usage: "exists <key>",
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
        name: "count",
        usage: "count",
    },
    CommandInfo {
        name: "help",
        usage: "help",
    },
    CommandInfo {
        name: "exit",
        usage: "exit",
    },
    CommandInfo {
        name: "snapshot",
        usage: "snapshot",
    },
];

pub fn command_info(name: &str) -> Option<&'static CommandInfo> {
    COMMANDS.iter().find(|command| command.name == name)
}

pub enum Command {
    Get { key: String },
    Set { key: String, value: String },
    Delete { keys: Vec<String> },
    Exists { key: String },
    List,
    Clear,
    Count,
    Help,
    Exit,
    Snapshot,
}
