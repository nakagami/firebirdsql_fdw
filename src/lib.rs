use pgrx::prelude::*;

pg_module_magic!();

pub mod fdw;

// Register the foreign data wrapper in the extension SQL.
pgrx::extension_sql!(
    r#"
CREATE FOREIGN DATA WRAPPER firebird_fdw
  HANDLER firebird_fdw_handler
  VALIDATOR firebird_fdw_validator;
"#,
    name = "create_firebird_fdw",
    requires = [],
    finalize,
);

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}
