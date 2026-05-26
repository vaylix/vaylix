pub enum Protocol {
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
