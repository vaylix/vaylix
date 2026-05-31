use command::COMMANDS;

pub(super) fn help_text() -> String {
    let name_width = COMMANDS
        .iter()
        .map(|command| command.name.len())
        .max()
        .unwrap_or(7)
        .max(7);
    let usage_width = COMMANDS
        .iter()
        .map(|command| command.usage.len())
        .max()
        .unwrap_or(5)
        .max(5);
    let mut lines = vec![
        "Vaylix command help".to_string(),
        "".to_string(),
        format!(
            "{:<name_width$} | {:<usage_width$}",
            "command",
            "usage",
            name_width = name_width,
            usage_width = usage_width
        ),
        format!("{}-+-{}", "-".repeat(name_width), "-".repeat(usage_width)),
    ];
    for command in COMMANDS {
        lines.push(format!(
            "{:<name_width$} | {:<usage_width$}",
            command.name,
            command.usage,
            name_width = name_width,
            usage_width = usage_width
        ));
    }

    lines.join("\n")
}
