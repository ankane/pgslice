## 0.6.1 (2023-04-26)

- Fixed `uninitialized constant` error

## 0.6.0 (2023-04-22)

- Added support for generated columns
- Added compression and extended statistics to `prep`
- Dropped support for Ruby < 2.7
- Dropped support for Postgres < 11

## 0.5.0 (2023-01-29)

- Dropped support for Ruby < 2.5

## 0.4.8 (2022-02-28)

- Fixed error with pg 1.3
- Reduced size of Docker image

## 0.4.7 (2020-08-14)

- Added `--tablespace` option to `add_partitions`
- Fixed sequence query if sequence in different schema than table

## 0.4.6 (2020-05-29)

- Ensure correct order with multi-column primary keys
- Ensure fill always uses correct date range (bug introduced in 0.4.5)

## 0.4.5 (2018-10-18)

- Added support for Postgres 11 foreign key improvements
- Improved versioning

## 0.4.4 (2018-08-18)

- Added partitioning by `year`
- Fixed `--source-table` and `--dest-table` options
- Added descriptions to options

## 0.4.3 (2018-08-16)

- Fixed sequence ownership
- Improved help

## 0.4.2 (2018-07-23)

- Added support for Postgres 11 index improvements
- Added support for all connection options

## 0.4.1 (2018-04-30)

- Better support for schemas
- Use latest partition for schema
- Added support for composite primary keys

## 0.4.0 (2017-10-07)

- Added support for declarative partitioning
- Added support for foreign keys

## 0.3.6 (2017-07-10)

- Fixed drop trigger on `unprep` for non-lowercase tables
- Fixed index creation for non-lowercase tables

## 0.3.5 (2017-07-06)

- Added support for non-lowercase tables and columns

## 0.3.4 (2017-07-06)

- Added `analyze` method
- Fixed `fill` with `--dry-run` option
- Better error message for tables without primary key

## 0.3.3 (2017-03-22)

- Fixed error when creating partitions

## 0.3.2 (2016-12-15)

- Exit with error code on interrupt
- Fixed `--start` option with `--swapped`

## 0.3.1 (2016-12-13)

- Fixed exception with `--no-partition` option
- Use proper cast type in `fill` method for legacy `timestamptz` columns

## 0.3.0 (2016-12-12)

- Better query performance for `timestamptz` columns
- Added support for schemas other than `public`

## 0.2.3 (2016-10-10)

- Added `--dest-table` option to `fill`
- Fixed errors with `fill` when no partitions created

## 0.2.2 (2016-10-06)

- Set `lock_timeout` on `swap` to prevent bad things from happening
- Friendlier error messages

## 0.2.1 (2016-09-28)

- Added `--where` option to `fill`
- Fixed partition detection with `fill`
- Fixed error for columns named `user` with `fill`

## 0.2.0 (2016-09-22)

- Switched to new trigger, which is about 20% faster

## 0.1.7 (2016-09-14)

- Added `--source-table` option to `fill`

## 0.1.6 (2016-08-04)

- Added `--no-partition` option to `prep`
- Added `--url` option

## 0.1.5 (2016-04-26)

- Removed `activesupport` dependency for speed
- Fixed `fill` for months

## 0.1.4 (2016-04-24)

- Added sequence ownership
- Default to 0 for `--past` and `--future` options
- Better `fill` with `--swapped`

## 0.1.3 (2016-04-24)

- Fixed table inheritance

## 0.1.2 (2016-04-24)

- Added `--dry-run` option
- Print sql to stdout instead of stderr

## 0.1.1 (2016-04-24)

- Added sql commands to output

## 0.1.0 (2016-04-24)

- First release
