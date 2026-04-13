use chrono::{Duration, NaiveDate, NaiveDateTime};
use chrono_tz::Tz;
use num_traits::ToPrimitive;
use pgrx::{
    datum::{Date, Timestamp, TimestampWithTimeZone},
    pg_sys::Oid,
    PgBuiltInOids, PgOid,
};
use std::collections::{HashMap, HashSet, VecDeque};

use supabase_wrappers::prelude::*;

use super::{FirebirdFdwError, FirebirdFdwResult};

// ──────────────────────────────────────────
// SQL helpers
// ──────────────────────────────────────────

/// Quote a Firebird identifier.  Firebird stores unquoted identifiers in
/// uppercase, so we uppercase here so that PostgreSQL column names like `id`
/// correctly resolve to Firebird's `ID`.
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.to_uppercase().replace('"', "\"\""))
}

/// Deparse a single Qual into a Firebird WHERE fragment.
fn deparse_qual(qual: &Qual, fmt: &mut FirebirdCellFormatter) -> String {
    let field = quote_ident(&qual.field);
    if qual.use_or {
        match &qual.value {
            Value::Cell(_) => unreachable!(),
            Value::Array(cells) => {
                let conds: Vec<String> = cells
                    .iter()
                    .map(|cell| format!("{} {} {}", field, qual.operator, fmt.fmt_cell(cell)))
                    .collect();
                conds.join(" OR ")
            }
        }
    } else {
        match &qual.value {
            Value::Cell(cell) => match qual.operator.as_str() {
                "is" | "is not" => match cell {
                    Cell::String(s) if s == "null" => {
                        format!("{} {} NULL", field, qual.operator.to_uppercase())
                    }
                    _ => format!("{} {} {}", field, qual.operator, fmt.fmt_cell(cell)),
                },
                "~~" => format!("{} LIKE {}", field, fmt.fmt_cell(cell)),
                "!~~" => format!("{} NOT LIKE {}", field, fmt.fmt_cell(cell)),
                _ => format!("{} {} {}", field, qual.operator, fmt.fmt_cell(cell)),
            },
            Value::Array(_) => unreachable!(),
        }
    }
}

// ──────────────────────────────────────────
// Cell → Firebird SQL literal formatter
// ──────────────────────────────────────────

struct FirebirdCellFormatter;

impl CellFormatter for FirebirdCellFormatter {
    fn fmt_cell(&mut self, cell: &Cell) -> String {
        match cell {
            Cell::Bool(b) => if *b { "TRUE".to_string() } else { "FALSE".to_string() },
            Cell::I8(v)  => v.to_string(),
            Cell::I16(v) => v.to_string(),
            Cell::I32(v) => v.to_string(),
            Cell::I64(v) => v.to_string(),
            Cell::F32(v) => v.to_string(),
            Cell::F64(v) => v.to_string(),
            Cell::Numeric(v) => v.to_string(),
            Cell::String(s) => format!("'{}'", s.replace('\'', "''")),
            Cell::Date(d) => {
                // d.to_pg_epoch_days() = days since 2000-01-01
                // Add 10957 to convert to days since 1970-01-01 (Unix epoch)
                let pg_days = d.to_pg_epoch_days();
                let unix_days = pg_days + 10957;
                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                let naive = epoch + Duration::days(unix_days as i64);
                format!("DATE '{}'", naive.format("%Y-%m-%d"))
            }
            Cell::Timestamp(ts) => {
                // ts.into_inner() = microseconds since PG epoch 2000-01-01
                const PG_TO_UNIX_MICROS: i64 = 10957 * 86400 * 1_000_000;
                let unix_micros = ts.into_inner() + PG_TO_UNIX_MICROS;
                let secs = unix_micros / 1_000_000;
                let ndt = chrono::DateTime::from_timestamp(secs, 0)
                    .map(|dt| dt.naive_utc())
                    .unwrap_or_default();
                format!("TIMESTAMP '{}'", ndt.format("%Y-%m-%d %H:%M:%S"))
            }
            Cell::Timestamptz(ts) => {
                const PG_TO_UNIX_MICROS: i64 = 10957 * 86400 * 1_000_000;
                let unix_micros = ts.into_inner() + PG_TO_UNIX_MICROS;
                let secs = unix_micros / 1_000_000;
                let ndt = chrono::DateTime::from_timestamp(secs, 0)
                    .map(|dt| dt.naive_utc())
                    .unwrap_or_default();
                format!("TIMESTAMP '{}'", ndt.format("%Y-%m-%d %H:%M:%S"))
            }
            Cell::Json(v) => format!("'{}'", v.0.to_string().replace('\'', "''")),
            // Firebird BLOB binary literals are complex; represent as NULL.
            Cell::Bytea(_) => "NULL".to_string(),
            // All other variants (Time, Interval, Uuid, arrays, …) are not
            // used by Firebird columns but must be handled for exhaustiveness.
            _ => "NULL".to_string(),
        }
    }
}

// ──────────────────────────────────────────
// Type mapping for IMPORT FOREIGN SCHEMA
// ──────────────────────────────────────────

const FB_SMALLINT: i16 = 7;
const FB_INTEGER: i16 = 8;
const FB_FLOAT: i16 = 10;
const FB_DATE: i16 = 12;
const FB_TIME: i16 = 13;
const FB_CHAR: i16 = 14;
const FB_BIGINT: i16 = 16;
const FB_BOOLEAN: i16 = 23;
const FB_DOUBLE: i16 = 27;
const FB_TIMESTAMP: i16 = 35;
const FB_VARCHAR: i16 = 37;
const FB_BLOB: i16 = 261;

fn firebird_type_to_pg(
    field_type: i16,
    field_sub_type: i16,
    field_precision: i16,
    field_scale: i16,
    _is_strict: bool,
) -> Option<String> {
    match field_type {
        FB_SMALLINT => {
            if field_sub_type > 0 && field_scale < 0 {
                let prec = if field_precision > 0 { field_precision } else { 4 };
                Some(format!("numeric({},{})", prec, -field_scale))
            } else {
                Some("smallint".to_string())
            }
        }
        FB_INTEGER => {
            if field_sub_type > 0 && field_scale < 0 {
                let prec = if field_precision > 0 { field_precision } else { 9 };
                Some(format!("numeric({},{})", prec, -field_scale))
            } else {
                Some("integer".to_string())
            }
        }
        FB_FLOAT => Some("real".to_string()),
        FB_DATE => Some("date".to_string()),
        FB_TIME => Some("time".to_string()),
        FB_CHAR | FB_VARCHAR => Some("text".to_string()),
        FB_BIGINT => {
            if field_scale < 0 {
                let prec = if field_precision > 0 { field_precision } else { 18 };
                let scale = -field_scale;
                Some(format!("numeric({prec},{scale})"))
            } else {
                Some("bigint".to_string())
            }
        }
        FB_BOOLEAN => Some("boolean".to_string()),
        FB_DOUBLE => Some("double precision".to_string()),
        FB_TIMESTAMP => Some("timestamp".to_string()),
        FB_BLOB => {
            if field_sub_type == 1 {
                Some("text".to_string())
            } else {
                Some("bytea".to_string())
            }
        }
        _ => None,
    }
}

// ──────────────────────────────────────────
// FDW struct
// ──────────────────────────────────────────

#[wrappers_fdw(
    version = "0.1.0",
    author = "Hajime Nakagami",
    website = "https://github.com/nakagami/firebirdsql_fdw",
    error_type = "FirebirdFdwError"
)]
pub(crate) struct FirebirdFdw {
    host: String,
    port: u16,
    db_name: String,
    username: String,
    password: String,

    conn: Option<firebirust::Connection>,

    table: String,
    rowid_col: String,
    tgt_cols: Vec<Column>,
    sql_query: String,

    rows: VecDeque<Vec<Option<Cell>>>,
    scanned_row_cnt: usize,
}

impl FirebirdFdw {
    #[allow(dead_code)]
    const FDW_NAME: &'static str = "FirebirdFdw";

    fn connect(&mut self) -> FirebirdFdwResult<()> {
        if self.conn.is_some() {
            return Ok(());
        }
        // firebirust::Connection::connect requires these keys to be present in opts;
        // it accesses them with the index operator and panics on missing keys.
        let mut opts: HashMap<String, String> = HashMap::new();
        opts.insert("role".to_string(), "".to_string());
        opts.insert("timezone".to_string(), "".to_string());
        opts.insert("wire_crypt".to_string(), "true".to_string());
        opts.insert("auth_plugin_name".to_string(), "Srp256".to_string());
        opts.insert("page_size".to_string(), "4096".to_string());
        let conn = firebirust::Connection::connect(
            &self.host,
            self.port,
            &self.db_name,
            &self.username,
            &self.password,
            &opts,
        )?;
        self.conn = Some(conn);
        Ok(())
    }

    fn disconnect(&mut self) {
        self.conn = None;
    }

    /// Build the SELECT SQL for the current scan parameters.
    fn deparse(
        &self,
        quals: &[Qual],
        columns: &[Column],
        sorts: &[Sort],
        limit: &Option<Limit>,
    ) -> String {
        // Firebird uses FIRST n SKIP m before the select list (not LIMIT/OFFSET at end).
        let first_clause = limit
            .as_ref()
            .map(|lim| {
                if lim.offset > 0 {
                    format!("FIRST {} SKIP {} ", lim.count, lim.offset)
                } else {
                    format!("FIRST {} ", lim.count)
                }
            })
            .unwrap_or_default();

        let tgts = if columns.is_empty() {
            "*".to_string()
        } else {
            columns
                .iter()
                .map(|c| quote_ident(&c.name))
                .collect::<Vec<_>>()
                .join(", ")
        };

        let table = if self.table.starts_with('(') && self.table.ends_with(')') {
            self.table.clone()
        } else {
            quote_ident(&self.table)
        };

        let mut sql = format!("SELECT {first_clause}{tgts} FROM {table}");

        if !quals.is_empty() {
            let mut fmt = FirebirdCellFormatter;
            let cond = quals
                .iter()
                .map(|q| deparse_qual(q, &mut fmt))
                .collect::<Vec<_>>()
                .join(" AND ");
            if !cond.is_empty() {
                sql.push_str(&format!(" WHERE {cond}"));
            }
        }

        if !sorts.is_empty() {
            let order_by = sorts
                .iter()
                .map(|s| {
                    let dir = if s.reversed { "DESC" } else { "ASC" };
                    format!("{} {}", quote_ident(&s.field), dir)
                })
                .collect::<Vec<_>>()
                .join(", ");
            sql.push_str(&format!(" ORDER BY {order_by}"));
        }

        sql
    }

    /// Execute `self.sql_query` and pre-fetch all rows into `self.rows`.
    ///
    /// Cell extraction is done inline so that Rust can infer the private
    /// `firebirust::query_result::Row` type without naming it.
    fn execute_query(&mut self) -> FirebirdFdwResult<()> {
        let sql = self.sql_query.clone();
        let tgt_cols = self.tgt_cols.clone();

        let conn = self.conn.as_mut().ok_or(FirebirdFdwError::NoConnection)?;

        // Block scope ends stmt's borrow on conn before we iterate.
        let result = {
            let mut stmt = conn.prepare(&sql)?;
            stmt.query(())?
        };

        let mut rows: VecDeque<Vec<Option<Cell>>> = VecDeque::new();

        for fb_row in result {
            let mut cells: Vec<Option<Cell>> = Vec::with_capacity(tgt_cols.len());

            for (idx, col) in tgt_cols.iter().enumerate() {
                let col_name = &col.name;

                // Local macro: call row.get::<Option<T>>(idx) with error mapping.
                // The concrete row type is inferred from the for-loop iterator.
                macro_rules! get {
                    ($ty:ty) => {
                        fb_row.get::<Option<$ty>>(idx).map_err(|e| {
                            FirebirdFdwError::ConversionError(col_name.clone(), format!("{e:?}"))
                        })
                    };
                }

                let cell: Option<Cell> = match PgOid::from(col.type_oid) {
                    PgOid::BuiltIn(PgBuiltInOids::BOOLOID) => get!(bool)?.map(Cell::Bool),
                    PgOid::BuiltIn(PgBuiltInOids::INT2OID) => get!(i16)?.map(Cell::I16),
                    PgOid::BuiltIn(PgBuiltInOids::INT4OID) => get!(i32)?.map(Cell::I32),
                    PgOid::BuiltIn(PgBuiltInOids::INT8OID) => get!(i64)?.map(Cell::I64),
                    PgOid::BuiltIn(PgBuiltInOids::FLOAT4OID) => get!(f32)?.map(Cell::F32),
                    PgOid::BuiltIn(PgBuiltInOids::FLOAT8OID) => get!(f64)?.map(Cell::F64),
                    PgOid::BuiltIn(PgBuiltInOids::NUMERICOID) => {
                        get!(rust_decimal::Decimal)?
                            .map(|d| {
                                let f = d.to_f64().unwrap_or(f64::NAN);
                                pgrx::AnyNumeric::try_from(f)
                            })
                            .transpose()
                            .map_err(FirebirdFdwError::PgrxNumericError)?
                            .map(Cell::Numeric)
                    }
                    PgOid::BuiltIn(
                        PgBuiltInOids::TEXTOID
                        | PgBuiltInOids::VARCHAROID
                        | PgBuiltInOids::BPCHAROID,
                    ) => get!(String)?.map(Cell::String),
                    PgOid::BuiltIn(PgBuiltInOids::JSONBOID) => {
                        match get!(String)? {
                            Some(s) => {
                                let v: serde_json::Value =
                                    serde_json::from_str(&s).map_err(|e| {
                                        FirebirdFdwError::ConversionError(
                                            col_name.clone(),
                                            format!("invalid JSON: {e}"),
                                        )
                                    })?;
                                Some(Cell::Json(pgrx::JsonB(v)))
                            }
                            None => None,
                        }
                    }
                    PgOid::BuiltIn(PgBuiltInOids::BYTEAOID) => {
                        match get!(Vec<u8>)? {
                            Some(b) => {
                                let pg_bytea = pgrx::rust_byte_slice_to_bytea(&b).into_pg();
                                Some(Cell::Bytea(pg_bytea))
                            }
                            None => None,
                        }
                    }
                    PgOid::BuiltIn(PgBuiltInOids::DATEOID) => {
                        match get!(NaiveDate)? {
                            Some(v) => {
                                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                                let unix_days = v.signed_duration_since(epoch).num_days() as i32;
                                // PG epoch = 2000-01-01 = 10957 days after Unix epoch
                                let pg_epoch_days = unix_days - 10957;
                                Some(Cell::Date(unsafe {
                                    Date::from_pg_epoch_days(pg_epoch_days)
                                }))
                            }
                            None => None,
                        }
                    }
                    PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPOID) => {
                        match get!(NaiveDateTime)? {
                            Some(v) => {
                                let unix_micros = v.and_utc().timestamp() * 1_000_000
                                    + v.and_utc().timestamp_subsec_micros() as i64;
                                const PG_TO_UNIX_MICROS: i64 = 10957 * 86400 * 1_000_000;
                                let pg_micros = unix_micros - PG_TO_UNIX_MICROS;
                                Some(Cell::Timestamp(
                                    Timestamp::try_from(pg_micros)
                                        .unwrap_or_else(|_| Timestamp::positive_infinity()),
                                ))
                            }
                            None => None,
                        }
                    }
                    PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPTZOID) => {
                        match get!(chrono::DateTime<Tz>)? {
                            Some(v) => {
                                let unix_micros = v.timestamp() * 1_000_000
                                    + v.timestamp_subsec_micros() as i64;
                                const PG_TO_UNIX_MICROS: i64 = 10957 * 86400 * 1_000_000;
                                let pg_micros = unix_micros - PG_TO_UNIX_MICROS;
                                Some(Cell::Timestamptz(
                                    TimestampWithTimeZone::try_from(pg_micros)
                                        .unwrap_or_else(|_| {
                                            TimestampWithTimeZone::positive_infinity()
                                        }),
                                ))
                            }
                            None => None,
                        }
                    }
                    _ => {
                        return Err(FirebirdFdwError::UnsupportedColumnType(col_name.clone()));
                    }
                };

                cells.push(cell);
            }

            rows.push_back(cells);
        }

        self.rows = rows;
        self.scanned_row_cnt = 0;
        Ok(())
    }
}

// ──────────────────────────────────────────
// ForeignDataWrapper trait impl
// ──────────────────────────────────────────

impl ForeignDataWrapper<FirebirdFdwError> for FirebirdFdw {
    fn new(server: ForeignServer) -> FirebirdFdwResult<Self> {
        let opts = &server.options;

        let host = require_option("host", opts)?.to_string();
        let port: u16 = opts
            .get("port")
            .and_then(|s| s.parse().ok())
            .unwrap_or(3050);
        let db_name = require_option("db_name", opts)?.to_string();
        let username = require_option("username", opts)?.to_string();
        let password = require_option("password", opts)?.to_string();

        Ok(FirebirdFdw {
            host,
            port,
            db_name,
            username,
            password,
            conn: None,
            table: String::new(),
            rowid_col: String::new(),
            tgt_cols: Vec::new(),
            sql_query: String::new(),
            rows: VecDeque::new(),
            scanned_row_cnt: 0,
        })
    }

    fn begin_scan(
        &mut self,
        quals: &[Qual],
        columns: &[Column],
        sorts: &[Sort],
        limit: &Option<Limit>,
        options: &HashMap<String, String>,
    ) -> FirebirdFdwResult<()> {
        self.table = require_option("table", options)?.to_string();
        self.tgt_cols = columns.to_vec();
        self.sql_query = self.deparse(quals, columns, sorts, limit);
        self.connect()?;
        self.execute_query()
    }

    fn iter_scan(&mut self, row: &mut Row) -> FirebirdFdwResult<Option<()>> {
        if let Some(cells) = self.rows.pop_front() {
            for (tgt_col, cell) in self.tgt_cols.iter().zip(cells) {
                row.push(&tgt_col.name, cell);
            }
            self.scanned_row_cnt += 1;
            return Ok(Some(()));
        }
        Ok(None)
    }

    fn re_scan(&mut self) -> FirebirdFdwResult<()> {
        self.execute_query()
    }

    fn end_scan(&mut self) -> FirebirdFdwResult<()> {
        self.disconnect();
        Ok(())
    }

    // ── DML ──────────────────────────────────────────────────────────────

    fn begin_modify(&mut self, options: &HashMap<String, String>) -> FirebirdFdwResult<()> {
        self.table = require_option("table", options)?.to_string();
        self.rowid_col = require_option("rowid_column", options)?.to_string();
        self.connect()
    }

    fn insert(&mut self, src: &Row) -> FirebirdFdwResult<()> {
        let mut fmt = FirebirdCellFormatter;
        let mut cols: Vec<String> = Vec::new();
        let mut vals: Vec<String> = Vec::new();

        for (col, cell) in src.iter() {
            cols.push(quote_ident(col));
            match cell {
                Some(c) => vals.push(fmt.fmt_cell(c)),
                None => vals.push("NULL".to_string()),
            }
        }

        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            quote_ident(&self.table),
            cols.join(", "),
            vals.join(", ")
        );

        let conn = self.conn.as_mut().ok_or(FirebirdFdwError::NoConnection)?;
        conn.execute_batch(&sql)?;
        conn.commit()?;
        Ok(())
    }

    fn update(&mut self, rowid: &Cell, new_row: &Row) -> FirebirdFdwResult<()> {
        let mut fmt = FirebirdCellFormatter;
        let mut sets: Vec<String> = Vec::new();

        for (col, cell) in new_row.iter() {
            if col == &self.rowid_col {
                continue;
            }
            let val = match cell {
                Some(c) => fmt.fmt_cell(c),
                None => "NULL".to_string(),
            };
            sets.push(format!("{} = {}", quote_ident(col), val));
        }

        let sql = format!(
            "UPDATE {} SET {} WHERE {} = {}",
            quote_ident(&self.table),
            sets.join(", "),
            quote_ident(&self.rowid_col),
            fmt.fmt_cell(rowid)
        );

        let conn = self.conn.as_mut().ok_or(FirebirdFdwError::NoConnection)?;
        conn.execute_batch(&sql)?;
        conn.commit()?;
        Ok(())
    }

    fn delete(&mut self, rowid: &Cell) -> FirebirdFdwResult<()> {
        let mut fmt = FirebirdCellFormatter;
        let sql = format!(
            "DELETE FROM {} WHERE {} = {}",
            quote_ident(&self.table),
            quote_ident(&self.rowid_col),
            fmt.fmt_cell(rowid)
        );

        let conn = self.conn.as_mut().ok_or(FirebirdFdwError::NoConnection)?;
        conn.execute_batch(&sql)?;
        conn.commit()?;
        Ok(())
    }

    fn end_modify(&mut self) -> FirebirdFdwResult<()> {
        self.disconnect();
        Ok(())
    }

    // ── IMPORT FOREIGN SCHEMA ─────────────────────────────────────────────

    fn import_foreign_schema(
        &mut self,
        stmt: ImportForeignSchemaStmt,
    ) -> FirebirdFdwResult<Vec<String>> {
        let is_strict =
            require_option_or("strict", &stmt.options, "false").eq_ignore_ascii_case("true");

        self.connect()?;

        let col_sql = "\
            SELECT TRIM(rf.RDB$RELATION_NAME), \
                   TRIM(rf.RDB$FIELD_NAME), \
                   f.RDB$FIELD_TYPE, \
                   COALESCE(f.RDB$FIELD_SUB_TYPE, 0), \
                   COALESCE(f.RDB$FIELD_PRECISION, 0), \
                   COALESCE(f.RDB$FIELD_SCALE, 0), \
                   COALESCE(rf.RDB$NULL_FLAG, 0) \
            FROM RDB$RELATION_FIELDS rf \
            JOIN RDB$FIELDS f ON rf.RDB$FIELD_SOURCE = f.RDB$FIELD_NAME \
            JOIN RDB$RELATIONS r ON rf.RDB$RELATION_NAME = r.RDB$RELATION_NAME \
            WHERE COALESCE(r.RDB$SYSTEM_FLAG, 0) = 0 \
            ORDER BY rf.RDB$RELATION_NAME, rf.RDB$FIELD_POSITION";

        let pk_sql = "\
            SELECT TRIM(rc.RDB$RELATION_NAME), \
                   TRIM(sg.RDB$FIELD_NAME) \
            FROM RDB$RELATION_CONSTRAINTS rc \
            JOIN RDB$INDEX_SEGMENTS sg ON rc.RDB$INDEX_NAME = sg.RDB$INDEX_NAME \
            WHERE rc.RDB$CONSTRAINT_TYPE = 'PRIMARY KEY'";

        let conn = self.conn.as_mut().ok_or(FirebirdFdwError::NoConnection)?;

        type ColInfo = Vec<(String, i16, i16, i16, i16, bool)>;
        let mut table_order: Vec<String> = Vec::new();
        let mut table_cols: HashMap<String, ColInfo> = HashMap::new();

        {
            let mut stmt = conn.prepare(col_sql)?;
            let result = stmt.query(())?;
            for row in result {
                let tbl: String = row.get::<Option<String>>(0)?.unwrap_or_default();
                let col: String = row.get::<Option<String>>(1)?.unwrap_or_default();
                let ftype: i16 = row.get::<Option<i16>>(2)?.unwrap_or(0);
                let sub: i16 = row.get::<Option<i16>>(3)?.unwrap_or(0);
                let prec: i16 = row.get::<Option<i16>>(4)?.unwrap_or(0);
                let scale: i16 = row.get::<Option<i16>>(5)?.unwrap_or(0);
                let null_flag: i16 = row.get::<Option<i16>>(6)?.unwrap_or(0);

                if !table_cols.contains_key(&tbl) {
                    table_order.push(tbl.clone());
                }
                table_cols
                    .entry(tbl)
                    .or_default()
                    .push((col, ftype, sub, prec, scale, null_flag != 0));
            }
        }

        let mut pk_map: HashMap<String, String> = HashMap::new();
        {
            let mut stmt = conn.prepare(pk_sql)?;
            let result = stmt.query(())?;
            for row in result {
                let tbl: String = row.get::<Option<String>>(0)?.unwrap_or_default();
                let col: String = row.get::<Option<String>>(1)?.unwrap_or_default();
                pk_map.entry(tbl).or_insert(col);
            }
        }

        let all_tables: HashSet<&str> = table_order.iter().map(|s| s.as_str()).collect();
        let table_list: HashSet<&str> = stmt.table_list.iter().map(|s| s.as_str()).collect();
        let selected: HashSet<&str> = match stmt.list_type {
            ImportSchemaType::FdwImportSchemaAll => all_tables,
            ImportSchemaType::FdwImportSchemaLimitTo => {
                all_tables.intersection(&table_list).copied().collect()
            }
            ImportSchemaType::FdwImportSchemaExcept => {
                all_tables.difference(&table_list).copied().collect()
            }
        };

        let mut ret: Vec<String> = Vec::new();

        for tbl_name in &table_order {
            if !selected.contains(tbl_name.as_str()) {
                continue;
            }
            let cols = match table_cols.get(tbl_name) {
                Some(c) => c,
                None => continue,
            };

            let mut fields: Vec<String> = Vec::new();
            let mut rowid_col: Option<String> = None;

            for (col_name, ftype, sub, prec, scale, not_null) in cols {
                match firebird_type_to_pg(*ftype, *sub, *prec, *scale, is_strict) {
                    Some(pg_type) => {
                        let nn = if *not_null { " NOT NULL" } else { "" };
                        let qcol = pgrx::spi::quote_identifier(col_name);
                        fields.push(format!("{qcol} {pg_type}{nn}"));
                        if pk_map.get(tbl_name).map(|pk| pk == col_name).unwrap_or(false)
                            && rowid_col.is_none()
                        {
                            rowid_col = Some(col_name.clone());
                        }
                    }
                    None => {
                        if is_strict {
                            return Err(FirebirdFdwError::UnsupportedColumnType(format!(
                                "{tbl_name}.{col_name}"
                            )));
                        }
                    }
                }
            }

            if !fields.is_empty() {
                let rowid_opt = rowid_col
                    .map(|r| format!(", rowid_column '{}'", r.replace('\'', "''")))
                    .unwrap_or_default();
                let tbl_ident = pgrx::spi::quote_identifier(tbl_name);
                let tbl_opt = tbl_name.replace('\'', "''");

                ret.push(format!(
                    "CREATE FOREIGN TABLE IF NOT EXISTS {tbl_ident} (\n  {}\n)\nSERVER {} OPTIONS (table '{tbl_opt}'{rowid_opt})",
                    fields.join(",\n  "),
                    stmt.server_name,
                ));
            }
        }

        self.disconnect();
        Ok(ret)
    }

    // ── validator ─────────────────────────────────────────────────────────

    fn validator(options: Vec<Option<String>>, catalog: Option<Oid>) -> FirebirdFdwResult<()> {
        if let Some(oid) = catalog {
            if oid == FOREIGN_TABLE_RELATION_ID {
                check_options_contain(&options, "table")?;
            }
        }
        Ok(())
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Unit tests (no PostgreSQL required)
// ──────────────────────────────────────────────────────────────────────────────
#[cfg(test)]
mod unit_tests {
    use super::{firebird_type_to_pg, quote_ident};
    use super::{
        FB_BIGINT, FB_BLOB, FB_BOOLEAN, FB_CHAR, FB_DATE, FB_DOUBLE, FB_FLOAT, FB_INTEGER,
        FB_SMALLINT, FB_TIMESTAMP, FB_TIME, FB_VARCHAR,
    };

    // ── quote_ident ───────────────────────────────────────────────────────────

    #[test]
    fn test_quote_ident_simple() {
        assert_eq!(quote_ident("EMPLOYEES"), r#""EMPLOYEES""#);
        // lowercase input is uppercased (Firebird stores unquoted idents in uppercase)
        assert_eq!(quote_ident("employees"), r#""EMPLOYEES""#);
    }

    #[test]
    fn test_quote_ident_with_double_quote() {
        // A double-quote inside an identifier must be escaped as ""
        assert_eq!(quote_ident(r#"COL"NAME"#), r#""COL""NAME""#);
    }

    #[test]
    fn test_quote_ident_empty() {
        assert_eq!(quote_ident(""), r#""""#);
    }

    // ── firebird_type_to_pg ───────────────────────────────────────────────────

    #[test]
    fn test_type_smallint() {
        assert_eq!(firebird_type_to_pg(FB_SMALLINT, 0, 0, 0, false), Some("smallint".into()));
    }

    #[test]
    fn test_type_integer() {
        assert_eq!(firebird_type_to_pg(FB_INTEGER, 0, 0, 0, false), Some("integer".into()));
    }

    #[test]
    fn test_type_bigint() {
        assert_eq!(firebird_type_to_pg(FB_BIGINT, 0, 0, 0, false), Some("bigint".into()));
    }

    #[test]
    fn test_type_numeric_from_bigint() {
        // NUMERIC(18,4) stored as BIGINT with field_scale=-4, field_precision=18
        assert_eq!(
            firebird_type_to_pg(FB_BIGINT, 0, 18, -4, false),
            Some("numeric(18,4)".into())
        );
    }

    #[test]
    fn test_type_numeric_from_integer() {
        // NUMERIC(7,2) stored as INTEGER (sub_type=1, scale=-2, precision=7)
        assert_eq!(
            firebird_type_to_pg(FB_INTEGER, 1, 7, -2, false),
            Some("numeric(7,2)".into())
        );
    }

    #[test]
    fn test_type_numeric_from_smallint() {
        // NUMERIC(4,2) stored as SMALLINT (sub_type=1, scale=-2, precision=4)
        assert_eq!(
            firebird_type_to_pg(FB_SMALLINT, 1, 4, -2, false),
            Some("numeric(4,2)".into())
        );
    }

    #[test]
    fn test_type_float() {
        assert_eq!(firebird_type_to_pg(FB_FLOAT, 0, 0, 0, false), Some("real".into()));
    }

    #[test]
    fn test_type_double() {
        assert_eq!(firebird_type_to_pg(FB_DOUBLE, 0, 0, 0, false), Some("double precision".into()));
    }

    #[test]
    fn test_type_char() {
        assert_eq!(firebird_type_to_pg(FB_CHAR, 0, 0, 0, false), Some("text".into()));
    }

    #[test]
    fn test_type_varchar() {
        assert_eq!(firebird_type_to_pg(FB_VARCHAR, 0, 0, 0, false), Some("text".into()));
    }

    #[test]
    fn test_type_date() {
        assert_eq!(firebird_type_to_pg(FB_DATE, 0, 0, 0, false), Some("date".into()));
    }

    #[test]
    fn test_type_time() {
        assert_eq!(firebird_type_to_pg(FB_TIME, 0, 0, 0, false), Some("time".into()));
    }

    #[test]
    fn test_type_timestamp() {
        assert_eq!(firebird_type_to_pg(FB_TIMESTAMP, 0, 0, 0, false), Some("timestamp".into()));
    }

    #[test]
    fn test_type_boolean() {
        assert_eq!(firebird_type_to_pg(FB_BOOLEAN, 0, 0, 0, false), Some("boolean".into()));
    }

    #[test]
    fn test_type_blob_text() {
        // sub_type=1 → text
        assert_eq!(firebird_type_to_pg(FB_BLOB, 1, 0, 0, false), Some("text".into()));
    }

    #[test]
    fn test_type_blob_binary() {
        // sub_type=0 → bytea
        assert_eq!(firebird_type_to_pg(FB_BLOB, 0, 0, 0, false), Some("bytea".into()));
    }

    #[test]
    fn test_type_unknown() {
        assert_eq!(firebird_type_to_pg(999, 0, 0, 0, false), None);
    }
}
