# pgslice

Postgres partitioning as easy as pie

## Install

Run

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

```sh
pgslice add_partitions <table> --future 3
```

## Additional Commands

To undo prep and delete partitions, use:

```sh
pgslice unprep <table>
```

To undo swap, use:

```sh
pgslice unswap <table>
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

## Contributing

Everyone is encouraged to help improve this project. Here are a few ways you can help:

- [Report bugs](https://github.com/ankane/pgslice/issues)
- Fix bugs and [submit pull requests](https://github.com/ankane/pgslice/pulls)
- Write, clarify, or fix documentation
- Suggest or add new features
