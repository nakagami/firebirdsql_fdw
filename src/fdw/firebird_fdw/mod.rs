#![allow(clippy::module_inception)]
mod firebird_fdw;

#[cfg(any(test, feature = "pg_test"))]
mod tests;

use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::PgSqlErrorCode;
use thiserror::Error;

use supabase_wrappers::prelude::{OptionsError};

#[derive(Error, Debug)]
pub(crate) enum FirebirdFdwError {
    #[error("column '{0}' data type is not supported")]
    UnsupportedColumnType(String),

    #[error("column '{0}' conversion error: {1}")]
    ConversionError(String, String),

    #[error("no active connection")]
    NoConnection,

    #[error("Firebird error: {0}")]
    FirebirdError(String),

    #[error("{0}")]
    PgrxNumericError(#[from] pgrx::datum::numeric_support::error::Error),

    #[error("{0}")]
    OptionsError(#[from] OptionsError),

    #[error("vault secret not found for id '{0}'")]
    #[allow(dead_code)]
    VaultSecretNotFound(String),
}

impl From<firebirust::Error> for FirebirdFdwError {
    fn from(e: firebirust::Error) -> Self {
        FirebirdFdwError::FirebirdError(format!("{e:?}"))
    }
}

impl From<FirebirdFdwError> for ErrorReport {
    fn from(value: FirebirdFdwError) -> Self {
        match value {
            FirebirdFdwError::OptionsError(e) => e.into(),
            FirebirdFdwError::FirebirdError(_) => ErrorReport::new(
                PgSqlErrorCode::ERRCODE_FDW_ERROR,
                "Firebird connection or query error",
                "check connection parameters and query syntax",
            ),
            other => ErrorReport::new(PgSqlErrorCode::ERRCODE_FDW_ERROR, format!("{other}"), ""),
        }
    }
}

pub(crate) type FirebirdFdwResult<T> = Result<T, FirebirdFdwError>;
