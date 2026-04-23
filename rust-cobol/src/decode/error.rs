use std::num::TryFromIntError;

#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    #[error(transparent)]
    IntConversion(#[from] TryFromIntError),
}
