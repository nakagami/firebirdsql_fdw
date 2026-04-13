-- Firebird initialisation script for integration tests.
--
-- Executed by the CI "seed" step (and by docker-compose via
-- /docker-entrypoint-initdb.d/) against the auto-created test.fdb database.
--
-- Schema must match the foreign-table definition in src/fdw/firebird_fdw/tests.rs:
--   CREATE FOREIGN TABLE fb_test_t1 (id INTEGER NOT NULL, name TEXT)
--   OPTIONS (table 'T1', rowid_column 'id');

CREATE TABLE T1 (
    id   INTEGER NOT NULL,
    name VARCHAR(255),
    CONSTRAINT pk_t1 PRIMARY KEY (id)
);

INSERT INTO T1 (id, name) VALUES (1, 'Alice');
INSERT INTO T1 (id, name) VALUES (2, 'Bob');
INSERT INTO T1 (id, name) VALUES (3, 'Charlie');

COMMIT;
