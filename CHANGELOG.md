## 0.4.1 [unreleased]

- Use latest partition for schema

## 0.4.0

- Added support for declarative partitioning
- Added support for foreign keys

## 0.3.6

- Fixed drop trigger on `unprep` for non-lowercase tables
- Fixed index creation for non-lowercase tables

## 0.3.5

- Added support for non-lowercase tables and columns

## 0.3.4

- Added `analyze` method
- Fixed `fill` with `--dry-run` option
- Better error message for tables without primary key

## 0.3.3

- Fixed error when creating partitions

## 0.3.2

- Exit with error code on interrupt
- Fixed `--start` option with `--swapped`

## 0.3.1

- Fixed exception with `--no-partition` option
- Use proper cast type in `fill` method for legacy `timestamptz` columns

## 0.3.0

- Better query performance for `timestamptz` columns
- Added support for schemas other than `public`

## 0.2.3

- Added `--dest-table` option to `fill`
- Fixed errors with `fill` when no partitions created

## 0.2.2

- Set `lock_timeout` on `swap` to prevent bad things from happening
- Friendlier error messages

## 0.2.1

- Added `--where` option to `fill`
- Fixed partition detection with `fill`
- Fixed error for columns named `user` with `fill`

## 0.2.0

- Switched to new trigger, which is about 20% faster

## 0.1.7

- Added `--source-table` option to `fill`

## 0.1.6

- Added `--no-partition` option to `prep`
- Added `--url` option

## 0.1.5

- Removed `activesupport` dependency for speed
- Fixed `fill` for months

## 0.1.4

- Added sequence ownership
- Default to 0 for `--past` and `--future` options
- Better `fill` with `--swapped`

## 0.1.3

- Fixed table inheritance

## 0.1.2

- Added `--dry-run` option
- Print sql to stdout instead of stderr

## 0.1.1

- Added sql commands to output

## 0.1.0

- First release
