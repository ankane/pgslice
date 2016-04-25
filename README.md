# pgslice

Postgres partitioning as easy as pie

## Install

Run:

```sh
gem install pgslice
```

## Steps

1. Specify your database credentials

  ```sh
  export PGSLICE_URL=postgres://localhost/myapp_development
  ```

2. Create an intermediate table

  ```sh
  pgslice prep <table> <column> <period>
  ```

  Period can be `day` or `month`.

  This creates a table named `<table>_intermediate` with the appropriate trigger for partitioning.

3. Add partitions

  ```sh
  pgslice add_partitions <table> --intermediate --past 3 --future 3
  ```

  This creates child tables that inherit from the intermediate table.

  Use the `--past` and `--future` options to control the number of partitions.

4. *Optional, for tables with data* - Fill the partitions in batches with data from the original table

  ```sh
  pgslice fill <table>
  ```

  Use the `--batch-size` and `--sleep` options to control the speed.

5. Swap the intermediate table with the original table

  ```sh
  pgslice swap <table>
  ```

  The original table is renamed `<table>_retired` and the intermediate table is renamed `<table>`.

6. Fill the rest

  ```sh
  pgslice fill <table> --swapped
  ```

7. Archive and drop the original table

## Adding Partitions

To add partitions, use:

```sh
pgslice add_partitions <table> --future 3
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

## Sample Output

`pgslice` prints the SQL commands that were executed on the server. To print without executing, use the `--dry-run` option.

```console
$ pgslice prep locations created_at day
BEGIN;

CREATE TABLE locations_intermediate (
  LIKE locations INCLUDING ALL
);

CREATE FUNCTION locations_insert_trigger()
RETURNS trigger AS $$
BEGIN
  EXECUTE 'INSERT INTO locations_' || to_char(NEW.created_at, 'YYYYMMDD') || ' VALUES ($1.*)' USING NEW;
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER locations_insert_trigger
BEFORE INSERT ON locations_intermediate
FOR EACH ROW EXECUTE PROCEDURE locations_insert_trigger();

COMMIT;
```

```console
$ pgslice add_partitions locations --intermediate --past 1 --future 1
BEGIN;

CREATE TABLE locations_20160423 (
  CHECK (created_at >= '2016-04-23'::date AND created_at < '2016-04-24'::date)
) INHERITS (locations_intermediate);

ALTER TABLE locations_20160423 ADD PRIMARY KEY (id);

CREATE INDEX ON locations_20160423 USING btree (updated_at, shopper_id);

CREATE TABLE locations_20160424 (
  CHECK (created_at >= '2016-04-24'::date AND created_at < '2016-04-25'::date)
) INHERITS (locations_intermediate);

ALTER TABLE locations_20160424 ADD PRIMARY KEY (id);

CREATE INDEX ON locations_20160424 USING btree (updated_at, shopper_id);

CREATE TABLE locations_20160425 (
  CHECK (created_at >= '2016-04-25'::date AND created_at < '2016-04-26'::date)
) INHERITS (locations_intermediate);

ALTER TABLE locations_20160425 ADD PRIMARY KEY (id);

CREATE INDEX ON locations_20160425 USING btree (updated_at, shopper_id);

COMMIT;
```

```console
$ pgslice swap locations
BEGIN;

ALTER TABLE locations RENAME TO locations_retired;

ALTER TABLE locations_intermediate RENAME TO locations;

ALTER SEQUENCE locations_id_seq OWNED BY locations.id;

COMMIT;
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

## TODO

- Command to sync index changes with partitions
- Disable indexing for faster `fill`

## Contributing

Everyone is encouraged to help improve this project. Here are a few ways you can help:

- [Report bugs](https://github.com/ankane/pgslice/issues)
- Fix bugs and [submit pull requests](https://github.com/ankane/pgslice/pulls)
- Write, clarify, or fix documentation
- Suggest or add new features
