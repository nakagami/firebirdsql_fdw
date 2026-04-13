# firebirdsql_fdw

A Firebird Foreign Data Wrapper (FDW) for PostgreSQL, written in Rust using
[pgrx](https://github.com/pgcentralfoundation/pgrx) and
[supabase/wrappers](https://github.com/supabase/wrappers).

The Firebird client driver is [firebirust](https://crates.io/crates/firebirust).

## Features

- Full `SELECT` with predicate pushdown (WHERE / ORDER BY / LIMIT)
- `INSERT`, `UPDATE`, `DELETE`
- `IMPORT FOREIGN SCHEMA` (auto-discovers tables and columns)
- Maps all common Firebird types to PostgreSQL equivalents

## Requirements

| Tool | Version |
|------|---------|
| Rust | stable ≥ 1.75 |
| cargo-pgrx | = 0.16.1 |
| PostgreSQL dev headers | 13 – 18 |
| gcc **or** clang | any recent version |
| libclang | any recent version |

On Ubuntu/Debian you may also need:

```bash
sudo apt-get install libreadline-dev flex bison
```

The compiler internal include path (needed by `bindgen` to locate `stddef.h`) is
resolved automatically at build time via `build.rs`: it tries
`gcc -print-libgcc-file-name` first, then falls back to `clang -print-resource-dir`.
No manual configuration is required and the build works on any architecture
(x86\_64, aarch64, …).

## Building

```bash
# One-time pgrx setup (builds PostgreSQL from source, ~5 min)
cargo pgrx init --pg18 download

# Type-check
cargo check --features pg18

# Build the extension
cargo pgrx package --features pg18

# Install into the pgrx-managed PostgreSQL and start a psql session
cargo pgrx run pg18
```

## Usage

```sql
CREATE EXTENSION firebirdsql_fdw;

CREATE SERVER my_firebird
  FOREIGN DATA WRAPPER firebird_fdw
  OPTIONS (
    host     'localhost',
    port     '3050',
    db_name  '/firebird/data/mydb.fdb',
    username 'SYSDBA',
    password 'masterkey'
  );

-- Map a single table manually
CREATE FOREIGN TABLE employees (
  id    integer NOT NULL,
  name  text,
  hired date
)
SERVER my_firebird
OPTIONS (table 'EMPLOYEES', rowid_column 'ID');

-- Or import the whole schema at once
IMPORT FOREIGN SCHEMA firebird
  FROM SERVER my_firebird
  INTO public
  OPTIONS (strict 'false');

-- Query
SELECT * FROM employees WHERE id = 1;

-- DML
INSERT INTO employees (id, name, hired) VALUES (42, 'Ada', '2024-01-01');
UPDATE employees SET name = 'Ada L.' WHERE id = 42;
DELETE FROM employees WHERE id = 42;
```

## Foreign table options

| Option | Scope | Required | Description |
|--------|-------|----------|-------------|
| `host` | SERVER | yes | Firebird host name or IP |
| `port` | SERVER | no | TCP port (default `3050`) |
| `db_name` | SERVER | yes | Database path on the Firebird server |
| `username` | SERVER | yes | Firebird user name |
| `password` | SERVER | yes | Firebird password |
| `table` | FOREIGN TABLE | yes | Firebird table or view name |
| `rowid_column` | FOREIGN TABLE | for DML | Primary-key column used for UPDATE/DELETE |

## Integration tests

Start the Firebird container (seeds a test database automatically):

```bash
docker compose up -d
# Wait ~10 s, then:
cargo pgrx test pg18
```

## Type mapping

| Firebird type | PostgreSQL type |
|---------------|-----------------|
| `SMALLINT` | `smallint` |
| `INTEGER` | `integer` |
| `BIGINT` | `bigint` |
| `FLOAT` | `real` |
| `DOUBLE PRECISION` | `double precision` |
| `NUMERIC(p,s)` | `numeric(p,s)` |
| `CHAR`, `VARCHAR` | `text` |
| `DATE` | `date` |
| `TIME` | `time` |
| `TIMESTAMP` | `timestamp` |
| `BOOLEAN` | `boolean` |
| `BLOB SUB_TYPE TEXT` | `text` |
| `BLOB SUB_TYPE BINARY` | `bytea` |
