# Changelog

## 1.2.0
Fix "Specified key was too long; max key length is 3072 bytes" MySQL error on the `trigger` stream. [31](https://github.com/singer-io/tap-circle-ci/pull/31)

Unconstrained `id`-type string fields (`id`, `project_id`, `pipeline_definition_id`, `organization_id`) caused MySQL targets to allocate oversized VARCHAR columns for indexing (e.g. VARCHAR(255) in utf8mb4 reserves 1020 bytes/column). The `trigger` stream's 4-column composite primary key exceeded MySQL's 3072-byte index limit as a result. Since these fields are always 36-character CircleCI UUIDs, added `"maxLength": 36` to them in the `trigger` and `pipeline_definition` schemas, bounding each column to 144 bytes and keeping every composite key well under the limit. No `key_properties` changed and no data/table migration is required.

## 1.1.2
* Refactor client code for error handling [30](https://github.com/singer-io/tap-circle-ci/pull/30)

## 1.1.1
* Fix 5xx error handling [#26](https://github.com/singer-io/tap-circle-ci/pull/26)
* Library version Upgrade

## 1.1.0
* Add New streams into the tap [#19](https://github.com/singer-io/tap-circle-ci/pull/19)
* Library version Upgrade [#20](https://github.com/singer-io/tap-circle-ci/pull/20)

## 1.0.0
* TDL-22064 add integration tests [#10](https://github.com/singer-io/tap-circle-ci/pull/10)
* Fix Catalog/Stream Metadata issues [#13](https://github.com/singer-io/tap-circle-ci/pull/13)
* TDL-22062 api client upgrade [#14](https://github.com/singer-io/tap-circle-ci/pull/14)
* TDL-22066 Schema Changes [#15](https://github.com/singer-io/tap-circle-ci/pull/15)
* TDL-22353 class based implementation [#16](https://github.com/singer-io/tap-circle-ci/pull/16)

## 0.1.2
  * Fix a bookmark bug [#3](https://github.com/singer-io/tap-circle-ci/pull/3/)
  * Add a Circle config to run pylint
  
## 0.1.1
  * Add a MANIFEST file [#1](https://github.com/singer-io/tap-circle-ci/pull/1/)

## 0.1.0
  * Initial Commit
    
