## 0.2.2 [unreleased]

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
