# pgslice

Postgres partitioning as easy as pie. Works great for both new and existing tables, with zero downtime and minimal app changes. Archive older data on a rolling basis to keep your database size under control.

:tangerine: Battle-tested at [Instacart](https://www.instacart.com/opensource)

## Install

pgslice is a command line tool. To install, run:

```sh
gem install pgslice
```

This will give you the `pgslice` command.

## Steps

1. Ensure the table you want to partition has been created. We’ll refer to this as `<table>`.

2. Specify your database credentials

  ```sh
  export PGSLICE_URL=postgres://localhost/myapp_development
  ```

3. Create an intermediate table

  ```sh
  pgslice prep <table> <column> <period>
  ```

  Period can be `day` or `month`.

  This creates a table named `<table>_intermediate` with the appropriate trigger for partitioning.

4. Add partitions

  ```sh
  pgslice add_partitions <table> --intermediate --past 3 --future 3
  ```

  This creates child tables that inherit from the intermediate table.

  Use the `--past` and `--future` options to control the number of partitions.

5. *Optional, for tables with data* - Fill the partitions in batches with data from the original table

  ```sh
  pgslice fill <table>
  ```

  Use the `--batch-size` and `--sleep` options to control the speed.

  To sync data across different databases, check out [pgsync](https://github.com/ankane/pgsync).

6. Analyze tables

  ```sh
  pgslice analyze <table>
  ```

7. Swap the intermediate table with the original table

  ```sh
  pgslice swap <table>
  ```

  The original table is renamed `<table>_retired` and the intermediate table is renamed `<table>`.

8. Fill the rest (rows inserted between the first fill and the swap)

  ```sh
  pgslice fill <table> --swapped
  ```

9. Back up the retired table with a tool like [pg_dump](https://www.postgresql.org/docs/current/static/app-pgdump.html) and drop it

  ```sql
  pg_dump -c -Fc -t <table>_retired $PGSLICE_URL > <table>_retired.dump
  psql -c "DROP <table>_retired" $PGSLICE_URL
  ```

## Sample Output

pgslice prints the SQL commands that were executed on the server. To print without executing, use the `--dry-run` option.

```sh
pgslice prep visits created_at month
```

```sql
BEGIN;

CREATE TABLE visits_intermediate (LIKE visits INCLUDING ALL);

CREATE FUNCTION visits_insert_trigger()
    RETURNS trigger AS $$
    BEGIN
        RAISE EXCEPTION 'Create partitions first.';
    END;
    $$ LANGUAGE plpgsql;

CREATE TRIGGER visits_insert_trigger
    BEFORE INSERT ON visits_intermediate
    FOR EACH ROW EXECUTE PROCEDURE visits_insert_trigger();

COMMENT ON TRIGGER visits_insert_trigger ON visits_intermediate is 'column:created_at,period:month';

COMMIT;
```

```sh
pgslice add_partitions visits --intermediate --past 1 --future 1
```

```sql
BEGIN;

CREATE TABLE visits_201608
    (CHECK (created_at >= '2016-08-01'::date AND created_at < '2016-09-01'::date))
    INHERITS (visits_intermediate);

ALTER TABLE visits_201608 ADD PRIMARY KEY (id);

CREATE INDEX ON visits_201608 USING btree (user_id);

CREATE TABLE visits_201609
    (CHECK (created_at >= '2016-09-01'::date AND created_at < '2016-10-01'::date))
    INHERITS (visits_intermediate);

ALTER TABLE visits_201609 ADD PRIMARY KEY (id);

CREATE INDEX ON visits_201609 USING btree (user_id);

CREATE TABLE visits_201610
    (CHECK (created_at >= '2016-10-01'::date AND created_at < '2016-11-01'::date))
    INHERITS (visits_intermediate);

ALTER TABLE visits_201610 ADD PRIMARY KEY (id);

CREATE INDEX ON visits_201610 USING btree (user_id);

CREATE OR REPLACE FUNCTION visits_insert_trigger()
    RETURNS trigger AS $$
    BEGIN
        IF (NEW.created_at >= '2016-09-01'::date AND NEW.created_at < '2016-10-01'::date) THEN
            INSERT INTO visits_201609 VALUES (NEW.*);
        ELSIF (NEW.created_at >= '2016-10-01'::date AND NEW.created_at < '2016-11-01'::date) THEN
            INSERT INTO visits_201610 VALUES (NEW.*);
        ELSIF (NEW.created_at >= '2016-08-01'::date AND NEW.created_at < '2016-09-01'::date) THEN
            INSERT INTO visits_201608 VALUES (NEW.*);
        ELSE
            RAISE EXCEPTION 'Date out of range. Ensure partitions are created.';
        END IF;
        RETURN NULL;
    END;
    $$ LANGUAGE plpgsql;

COMMIT;
```

```sh
pgslice fill visits
```

```sql
/* 1 of 3 */
INSERT INTO visits_intermediate ("id", "user_id", "ip", "created_at")
    SELECT "id", "user_id", "ip", "created_at" FROM visits
    WHERE id > 0 AND id <= 10000 AND created_at >= '2016-08-01'::date AND created_at < '2016-11-01'::date

/* 2 of 3 */
INSERT INTO visits_intermediate ("id", "user_id", "ip", "created_at")
    SELECT "id", "user_id", "ip", "created_at" FROM visits
    WHERE id > 10000 AND id <= 20000 AND created_at >= '2016-08-01'::date AND created_at < '2016-11-01'::date

/* 3 of 3 */
INSERT INTO visits_intermediate ("id", "user_id", "ip", "created_at")
    SELECT "id", "user_id", "ip", "created_at" FROM visits
    WHERE id > 20000 AND id <= 30000 AND created_at >= '2016-08-01'::date AND created_at < '2016-11-01'::date
```

```sh
pgslice analyze visits
```

```sql
ANALYZE VERBOSE visits_201608;

ANALYZE VERBOSE visits_201609;

ANALYZE VERBOSE visits_201610;

ANALYZE VERBOSE visits_intermediate;
```

```sh
pgslice swap visits
```

```sql
BEGIN;

SET LOCAL lock_timeout = '5s';

ALTER TABLE visits RENAME TO visits_retired;

ALTER TABLE visits_intermediate RENAME TO visits;

ALTER SEQUENCE visits_id_seq OWNED BY visits.id;

COMMIT;
```

## Adding Partitions

To add partitions, use:

```sh
pgslice add_partitions <table> --future 3
```

Add this as a cron job to create a new partition each day or month.

```sh
# day
0 0 * * * pgslice add_partitions <table> --future 3 --url ...

# month
0 0 1 * * pgslice add_partitions <table> --future 3 --url ...
```

Add a monitor to ensure partitions are being created.

```sql
SELECT 1 FROM
    pg_catalog.pg_class c
INNER JOIN
    pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE
    c.relkind = 'r' AND
    n.nspname = 'public' AND
    c.relname = '<table>_' || to_char(NOW() + INTERVAL '3 days', 'YYYYMMDD')
    -- for months, use to_char(NOW() + INTERVAL '3 months', 'YYYYMM')
```

## Archiving Partitions

Back up and drop older partitions each day or month.

```sh
pg_dump -c -Fc -t <table>_201609 $PGSLICE_URL > <table>_201609.dump
psql -c "DROP <table>_201609" $PGSLICE_URL
```

If you use [Amazon S3](https://aws.amazon.com/s3/) for backups, [s3cmd](https://github.com/s3tools/s3cmd) is a nice tool.

```sh
s3cmd put <table>_201609.dump s3://<s3-bucket>/<table>_201609.dump
```

## Additional Commands

To undo prep (which will delete partitions), use:

```sh
pgslice unprep <table>
```

To undo swap, use:

```sh
pgslice unswap <table>
```

## App Considerations

This set up allows you to read and write with the original table name with no knowledge it’s partitioned. However, there are a few things to be aware of.

### Writes

If you use `INSERT` statements with a `RETURNING` clause (as frameworks like Rails do), you’ll no longer receive the id of the newly inserted record(s) back. If you need this, you can either:

1. Insert directly into the partition
2. Get value before the insert with `SELECT nextval('sequence_name')` (for multiple rows, append `FROM generate_series(1, n)`)

### Reads

When possible, queries should include the column you partition on to limit the number of partitions the database needs to check.  For instance, if you partition on `created_at`, try to include it in queries:

```sql
SELECT * FROM
    visits
WHERE
    user_id = 123 AND
    -- for performance
    created_at >= '2016-09-01' AND created_at < '2016-09-02'
```

For this to be effective, ensure `constraint_exclusion` is set to `partition` (default value) or `on`.

```sql
SHOW constraint_exclusion;
```

## One Off Tasks

You can also use pgslice to reduce the size of a table without partitioning by creating a new table, filling it with a subset of records, and swapping it in.

```sh
pgslice prep <table> --no-partition
pgslice fill <table> --where "id > 1000" # use any conditions
pgslice swap <table>
```

## Upgrading

Run:

```sh
gem install pgslice
```

To use master, run:

```sh
gem install specific_install
gem specific_install ankane/pgslice
```

## Docker

```sh
docker build -t pgslice .
alias pgslice="docker run --rm -e PGSLICE_URL pgslice"
```

This will give you the `pgslice` command.

## Reference

- [PostgreSQL Manual](https://www.postgresql.org/docs/current/static/ddl-partitioning.html)
- [PostgreSQL Wiki](https://wiki.postgresql.org/wiki/Table_partitioning)

## TODO

- Command to sync index changes with partitions
- Disable indexing for faster `fill`
- ETA for `fill`

## Related Projects

Also check out:

- [PgHero](https://github.com/ankane/pghero) - A performance dashboard for Postgres
- [pgsync](https://github.com/ankane/pgsync) - Sync Postgres data to your local machine

## Contributing

Everyone is encouraged to help improve this project. Here are a few ways you can help:

- [Report bugs](https://github.com/ankane/pgslice/issues)
- Fix bugs and [submit pull requests](https://github.com/ankane/pgslice/pulls)
- Write, clarify, or fix documentation
- Suggest or add new features
