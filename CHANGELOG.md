# Changelog

## 2.0.0
**BREAKING CHANGES**: Restored composite key_properties for multiple streams after discovering CircleCI API `id` field is NOT globally unique.

Test data analysis revealed that the same `id` values appear across different organizations/projects/workflows, meaning the `id` field is scoped, not globally unique. While CircleCI docs claim IDs are unique, they appear to refer to a different system UUID not returned in the API response. The composite keys are necessary for data integrity.

**Key Changes:**
* context: key_properties remains `["id", "organization_id"]` (id is scoped to organization)
* deploy: key_properties remains `["id", "organization_id"]` (id is scoped to organization)
* pipeline_definition: key_properties remains `["id", "project_id", "organization_id"]` (id is scoped within project+org)
* project: key_properties remains `["id", "organization_id"]` (id is scoped to organization)
* jobs: key_properties remains `["id", "_workflow_id"]` (id is scoped to workflow)
* trigger: key_properties remains `["id", "project_id", "pipeline_definition_id", "organization_id"]` (id is scoped within pipeline+project+org, consistent with sibling streams)
* schedule: key_properties changed from `["id", "project-slug"]` to `["id", "project_id"]` (uses correct API ID field); added `project_id` field to schema

**Fix for MySQL Key Length (SAC-31452)**: The original issue reported MySQL error "Specified key was too long; max key length is 3072 bytes" for the `trigger` table's 4-column composite key. Root cause: schema fields for `id`, `project_id`, `pipeline_definition_id`, and `organization_id` were unconstrained strings, causing MySQL targets to allocate oversized VARCHAR columns (e.g. VARCHAR(255) in utf8mb4 reserves 1020 bytes/column, and 4 such columns exceed the 3072-byte limit). Since these fields are always 36-character UUIDs, added `"maxLength": 36` to these fields in the `trigger` and `pipeline_definition` schemas, bounding each column to 144 bytes (utf8mb4) and keeping every composite key comfortably under the MySQL limit without sacrificing the composite keys required for uniqueness.

**Migration**: Records in target tables will require schema reinitialization for affected streams due to key changes.

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
    
