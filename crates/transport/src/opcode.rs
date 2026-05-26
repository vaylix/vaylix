use crate::TransportError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Opcode {
    Get = 0x01,
    Set = 0x02,
    Delete = 0x03,
    Exists = 0x04,
    List = 0x05,
    Clear = 0x06,
    Count = 0x07,
    Snapshot = 0x08,
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
            0x01 => Ok(Self::Get),
            0x02 => Ok(Self::Set),
            0x03 => Ok(Self::Delete),
            0x04 => Ok(Self::Exists),
            0x05 => Ok(Self::List),
            0x06 => Ok(Self::Clear),
            0x07 => Ok(Self::Count),
            0x08 => Ok(Self::Snapshot),
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
            (0x01, Opcode::Get),
            (0x02, Opcode::Set),
            (0x03, Opcode::Delete),
            (0x04, Opcode::Exists),
            (0x05, Opcode::List),
            (0x06, Opcode::Clear),
            (0x07, Opcode::Count),
            (0x08, Opcode::Snapshot),
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
