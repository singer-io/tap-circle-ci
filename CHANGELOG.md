# Changelog

## 2.0.0
**BREAKING CHANGE**: Fix "Specified key was too long; max key length is 3072 bytes" MySQL error on the `trigger` stream (SAC-31452). [31](https://github.com/singer-io/tap-circle-ci/pull/31)

The downstream cataloging step (used by some targets, e.g. MySQL/Qlik Replicate ingestion) does not honor JSON Schema `maxLength` for column sizing, so it allocates a fixed-width VARCHAR column per string field regardless of `maxLength`. The `trigger` stream's 4-column composite primary key (`id`, `project_id`, `pipeline_definition_id`, `organization_id`) exceeded the target's 3072-byte index limit as a result.

* `trigger`: `key_properties` changed from `["id", "project_id", "pipeline_definition_id", "organization_id"]` to `["id", "pipeline_definition_id", "organization_id"]`. `project_id` was dropped because it is fully determined by `pipeline_definition_id` (a pipeline_definition belongs to exactly one project) and never varies independently — confirmed empirically via duplicate pipeline_definition records observed across two CircleCI organizations, where `project_id` was identical in both. `organization_id` is retained because it is the column proven to actually disambiguate colliding `id`/`pipeline_definition_id` values across organizations.

**Migration**: The `trigger` table's primary key changes; downstream tables will need to be reinitialized to reflect the new key.

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
    
