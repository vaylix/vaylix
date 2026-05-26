#[derive(Debug, Clone)]
pub enum Response {
    Ok,

    Value(String),

    Count(usize),

    NotFound,

    Error { code: String, message: String },
}
