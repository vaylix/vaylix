use crate::TransportError;

/// Wire-level operation identifiers for client requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Opcode {
    Auth = 0x01,
    Ping = 0x02,
    Get = 0x03,
    GetDel = 0x04,
    GetEx = 0x05,
    Set = 0x06,
    SetNx = 0x07,
    Delete = 0x08,
    Exists = 0x09,
    MGet = 0x0A,
    MSet = 0x0B,
    Incr = 0x0C,
    Decr = 0x0D,
    Expire = 0x0E,
    Ttl = 0x0F,
    Persist = 0x10,
    Rename = 0x11,
    RenameNx = 0x12,
    Scan = 0x13,
    DbSize = 0x14,
    Info = 0x15,
    Metrics = 0x16,
    List = 0x17,
    Clear = 0x18,
    Count = 0x19,
    Save = 0x1A,
    Snapshot = 0x1B,
    Multi = 0x1C,
    Exec = 0x1D,
    Discard = 0x1E,
    Backup = 0x1F,
    Restore = 0x20,
    CreateUser = 0x21,
    DropUser = 0x22,
    CreateRole = 0x23,
    DropRole = 0x24,
    GrantRole = 0x25,
    RevokeRole = 0x26,
    GrantPermission = 0x27,
    RevokePermission = 0x28,
    ShowUsers = 0x29,
    ShowRoles = 0x2A,
    WhoAmI = 0x2B,
}

impl From<Opcode> for u8 {
    fn from(value: Opcode) -> Self {
        value as u8
    }
}

impl TryFrom<u8> for Opcode {
    type Error = TransportError;

    fn try_from(value: u8) -> std::result::Result<Self, Self::Error> {
        match value {
            0x01 => Ok(Self::Auth),
            0x02 => Ok(Self::Ping),
            0x03 => Ok(Self::Get),
            0x04 => Ok(Self::GetDel),
            0x05 => Ok(Self::GetEx),
            0x06 => Ok(Self::Set),
            0x07 => Ok(Self::SetNx),
            0x08 => Ok(Self::Delete),
            0x09 => Ok(Self::Exists),
            0x0A => Ok(Self::MGet),
            0x0B => Ok(Self::MSet),
            0x0C => Ok(Self::Incr),
            0x0D => Ok(Self::Decr),
            0x0E => Ok(Self::Expire),
            0x0F => Ok(Self::Ttl),
            0x10 => Ok(Self::Persist),
            0x11 => Ok(Self::Rename),
            0x12 => Ok(Self::RenameNx),
            0x13 => Ok(Self::Scan),
            0x14 => Ok(Self::DbSize),
            0x15 => Ok(Self::Info),
            0x16 => Ok(Self::Metrics),
            0x17 => Ok(Self::List),
            0x18 => Ok(Self::Clear),
            0x19 => Ok(Self::Count),
            0x1A => Ok(Self::Save),
            0x1B => Ok(Self::Snapshot),
            0x1C => Ok(Self::Multi),
            0x1D => Ok(Self::Exec),
            0x1E => Ok(Self::Discard),
            0x1F => Ok(Self::Backup),
            0x20 => Ok(Self::Restore),
            0x21 => Ok(Self::CreateUser),
            0x22 => Ok(Self::DropUser),
            0x23 => Ok(Self::CreateRole),
            0x24 => Ok(Self::DropRole),
            0x25 => Ok(Self::GrantRole),
            0x26 => Ok(Self::RevokeRole),
            0x27 => Ok(Self::GrantPermission),
            0x28 => Ok(Self::RevokePermission),
            0x29 => Ok(Self::ShowUsers),
            0x2A => Ok(Self::ShowRoles),
            0x2B => Ok(Self::WhoAmI),
            other => Err(TransportError::UnknownOpcode(other)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Opcode;
    use crate::TransportError;

    #[test]
    fn converts_all_known_opcodes() {
        let mappings = [
            (0x01, Opcode::Auth),
            (0x02, Opcode::Ping),
            (0x03, Opcode::Get),
            (0x04, Opcode::GetDel),
            (0x05, Opcode::GetEx),
            (0x06, Opcode::Set),
            (0x07, Opcode::SetNx),
            (0x08, Opcode::Delete),
            (0x09, Opcode::Exists),
            (0x0A, Opcode::MGet),
            (0x0B, Opcode::MSet),
            (0x0C, Opcode::Incr),
            (0x0D, Opcode::Decr),
            (0x0E, Opcode::Expire),
            (0x0F, Opcode::Ttl),
            (0x10, Opcode::Persist),
            (0x11, Opcode::Rename),
            (0x12, Opcode::RenameNx),
            (0x13, Opcode::Scan),
            (0x14, Opcode::DbSize),
            (0x15, Opcode::Info),
            (0x16, Opcode::Metrics),
            (0x17, Opcode::List),
            (0x18, Opcode::Clear),
            (0x19, Opcode::Count),
            (0x1A, Opcode::Save),
            (0x1B, Opcode::Snapshot),
            (0x1C, Opcode::Multi),
            (0x1D, Opcode::Exec),
            (0x1E, Opcode::Discard),
            (0x1F, Opcode::Backup),
            (0x20, Opcode::Restore),
            (0x21, Opcode::CreateUser),
            (0x22, Opcode::DropUser),
            (0x23, Opcode::CreateRole),
            (0x24, Opcode::DropRole),
            (0x25, Opcode::GrantRole),
            (0x26, Opcode::RevokeRole),
            (0x27, Opcode::GrantPermission),
            (0x28, Opcode::RevokePermission),
            (0x29, Opcode::ShowUsers),
            (0x2A, Opcode::ShowRoles),
            (0x2B, Opcode::WhoAmI),
        ];

        for (byte, opcode) in mappings {
            assert_eq!(Opcode::try_from(byte).unwrap(), opcode);
            assert_eq!(u8::from(opcode), byte);
        }
    }

    #[test]
    fn rejects_unknown_opcode() {
        assert!(matches!(
            Opcode::try_from(0xff),
            Err(TransportError::UnknownOpcode(0xff))
        ));
    }
}
