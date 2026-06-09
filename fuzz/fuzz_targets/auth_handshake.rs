#![no_main]

use command::{Command, Parser};
use libfuzzer_sys::fuzz_target;
use transport::{Opcode, Request};
use uuid::Uuid;

fuzz_target!(|data: &[u8]| {
    let capped = &data[..data.len().min(1024)];
    let split = capped.len() / 2;
    let username = String::from_utf8_lossy(&capped[..split]).into_owned();
    let password = String::from_utf8_lossy(&capped[split..]).into_owned();

    let command = Command::Auth {
        username: username.clone(),
        password: password.clone(),
    };
    if let Ok(request) = Request::from_command(Uuid::from_u128(1), command)
        && let Ok(Command::Auth {
            username: decoded_user,
            password: decoded_password,
        }) = request.into_command()
    {
        assert_eq!(decoded_user, username);
        assert_eq!(decoded_password, password);
    }

    let raw_auth = Request::new(Uuid::from_u128(2), Opcode::Auth, capped.to_vec());
    let _ = raw_auth.into_command();

    let auth_text = format!("auth \"{}\" \"{}\"", escape_cli(&username), escape_cli(&password));
    let _ = Parser::parse(&auth_text);
});

fn escape_cli(value: &str) -> String {
    value
        .chars()
        .take(128)
        .flat_map(|ch| match ch {
            '\\' => ['\\', '\\'],
            '"' => ['\\', '"'],
            _ => [ch, '\0'],
        })
        .filter(|ch| *ch != '\0')
        .collect()
}
