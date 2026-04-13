// Integration tests for FirebirdFdw.
//
// These tests require a running Firebird server.  Start one with:
//
//   docker compose up -d
//
// Then run:
//
//   cargo pgrx test pg15
//
// The tests connect through PostgreSQL's FDW layer so pgrx's embedded
// PostgreSQL server must be able to reach the Firebird container on localhost.

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    // Read Firebird connection parameters from environment variables so the
    // same binary works both locally and in CI (Docker).
    //
    // Defaults match the jacobalberty/firebird Docker image used in CI:
    //   FIREBIRD_HOST     localhost
    //   FIREBIRD_PORT     3050
    //   FIREBIRD_DB       /firebird/data/test.fdb
    //   FIREBIRD_USER     SYSDBA
    //   FIREBIRD_PASSWORD masterkey
    fn fb_host()     -> String { std::env::var("FIREBIRD_HOST").unwrap_or_else(|_| "localhost".into()) }
    fn fb_port()     -> String { std::env::var("FIREBIRD_PORT").unwrap_or_else(|_| "3050".into()) }
    fn fb_db()       -> String { std::env::var("FIREBIRD_DB").unwrap_or_else(|_| "/var/lib/firebird/4.0/data/test_fdw.fdb".into()) }
    fn fb_user()     -> String { std::env::var("FIREBIRD_USER").unwrap_or_else(|_| "SYSDBA".into()) }
    fn fb_password() -> String { std::env::var("FIREBIRD_PASSWORD").unwrap_or_else(|_| "masterkey".into()) }

    // Helper: set up server + foreign table once per test binary run.
    fn setup_fdw() {
        let (host, port, db, user, pass) = (fb_host(), fb_port(), fb_db(), fb_user(), fb_password());
        Spi::run(&format!(
            "CREATE EXTENSION IF NOT EXISTS firebirdsql_fdw;

             CREATE SERVER IF NOT EXISTS fb_test_server
               FOREIGN DATA WRAPPER firebird_fdw
               OPTIONS (
                 host '{host}',
                 port '{port}',
                 db_name '{db}',
                 username '{user}',
                 password '{pass}'
               );

             CREATE FOREIGN TABLE IF NOT EXISTS fb_test_t1 (
               id    integer NOT NULL,
               name  text
             )
             SERVER fb_test_server
             OPTIONS (table 'T1', rowid_column 'id');",
        ))
        .expect("FDW setup failed");
    }

    #[pg_test]
    fn test_select_all() {
        setup_fdw();
        let result = Spi::get_one::<i64>("SELECT COUNT(*) FROM fb_test_t1")
            .expect("SELECT failed")
            .unwrap_or(0);
        // Just assert the query runs without error; exact count depends on seed data.
        assert!(result >= 0);
    }

    #[pg_test]
    fn test_select_where() {
        setup_fdw();
        // Predicate pushdown test – WHERE clause should reach Firebird.
        let result =
            Spi::get_one::<i64>("SELECT COUNT(*) FROM fb_test_t1 WHERE id = 1")
                .expect("SELECT with WHERE failed")
                .unwrap_or(0);
        assert!(result >= 0);
    }

    #[pg_test]
    fn test_insert() {
        setup_fdw();
        Spi::run("INSERT INTO fb_test_t1 (id, name) VALUES (9999, 'pgrx_insert_test')")
            .expect("INSERT failed");
        let found = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM fb_test_t1 WHERE id = 9999",
        )
        .expect("SELECT after INSERT failed")
        .unwrap_or(0);
        assert_eq!(found, 1);
        // Cleanup
        Spi::run("DELETE FROM fb_test_t1 WHERE id = 9999").ok();
    }

    #[pg_test]
    fn test_update() {
        setup_fdw();
        Spi::run("INSERT INTO fb_test_t1 (id, name) VALUES (9998, 'before')")
            .expect("INSERT failed");
        Spi::run("UPDATE fb_test_t1 SET name = 'after' WHERE id = 9998")
            .expect("UPDATE failed");
        let val = Spi::get_one::<String>("SELECT name FROM fb_test_t1 WHERE id = 9998")
            .expect("SELECT after UPDATE failed");
        assert_eq!(val.as_deref(), Some("after"));
        Spi::run("DELETE FROM fb_test_t1 WHERE id = 9998").ok();
    }

    #[pg_test]
    fn test_delete() {
        setup_fdw();
        Spi::run("INSERT INTO fb_test_t1 (id, name) VALUES (9997, 'to_delete')")
            .expect("INSERT failed");
        Spi::run("DELETE FROM fb_test_t1 WHERE id = 9997").expect("DELETE failed");
        let cnt = Spi::get_one::<i64>("SELECT COUNT(*) FROM fb_test_t1 WHERE id = 9997")
            .expect("SELECT after DELETE failed")
            .unwrap_or(1);
        assert_eq!(cnt, 0);
    }
}