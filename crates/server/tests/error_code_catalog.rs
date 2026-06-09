use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

#[test]
fn source_error_codes_are_documented_and_unique() {
    let root = workspace_root();
    let docs = fs::read_to_string(root.join("ERROR_CODES.md")).expect("ERROR_CODES.md should load");
    let documented = extract_error_codes(&docs);
    let sources = [
        "crates/command/src/error.rs",
        "crates/engine/src/error.rs",
        "crates/transport/src/error.rs",
        "crates/server/src/error.rs",
        "crates/server/src/server/commands.rs",
        "crates/client/src/error.rs",
    ];

    let mut source_codes = BTreeSet::new();
    for source in sources {
        let text = fs::read_to_string(root.join(source)).expect("source file should load");
        source_codes.extend(extract_error_codes(&text));
    }

    let undocumented = source_codes
        .difference(&documented)
        .cloned()
        .collect::<Vec<_>>();
    assert!(
        undocumented.is_empty(),
        "source error codes missing from ERROR_CODES.md: {undocumented:?}"
    );

    let duplicates = duplicate_error_codes(&docs);
    assert!(
        duplicates.is_empty(),
        "duplicate codes in ERROR_CODES.md: {duplicates:?}"
    );
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("server crate should live under crates/server")
        .to_path_buf()
}

fn extract_error_codes(text: &str) -> BTreeSet<String> {
    let mut codes = BTreeSet::new();
    for token in
        text.split(|ch: char| !(ch.is_ascii_uppercase() || ch.is_ascii_digit() || ch == '-'))
    {
        if is_error_code(token) {
            codes.insert(token.to_string());
        }
    }
    codes
}

fn duplicate_error_codes(text: &str) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut duplicates = BTreeSet::new();
    for token in
        text.split(|ch: char| !(ch.is_ascii_uppercase() || ch.is_ascii_digit() || ch == '-'))
    {
        if is_error_code(token) && !seen.insert(token.to_string()) {
            duplicates.insert(token.to_string());
        }
    }
    duplicates.into_iter().collect()
}

fn is_error_code(token: &str) -> bool {
    let bytes = token.as_bytes();
    bytes.len() == 7
        && bytes[0..3].iter().all(u8::is_ascii_uppercase)
        && bytes[3] == b'-'
        && bytes[4..7].iter().all(u8::is_ascii_digit)
}
