# dbt-impala

The `dbt-impala` adapter allows you to use [dbt](https://www.getdbt.com/) along with [Apache Impala](https://impala.apache.org/) and [Cloudera Data Platform](https://cloudera.com)


## Getting started

- [Install dbt](https://docs.getdbt.com/docs/installation)
- Read the [introduction](https://docs.getdbt.com/docs/introduction/) and [viewpoint](https://docs.getdbt.com/docs/about/viewpoint/)

### Requirements

Current version of dbt-impala work only with dbt-core 1.3 but not with dbt-core >= 1.4.
We are actively working on next release 1.4 which will work with dbt-core 1.4

Python >= 3.7
dbt-core == 1.3.*

For development/testing or contribution to the dbt-impala, please follow [Contributing](CONTRIBUTING.md) guidelines.

### Installing dbt-impala

`pip install dbt-impala`

### Profile Setup

```
demo_project:
  target: dev
  outputs:
    dev:
     type: impala
     host: impala-coordinator.my.org.com
     port: 443
     dbname: my_db
     schema: my_db
     user: my_user
     password: my_pass
     auth_type: ldap
     http_path: cliservice
```

## Supported features
| Name | Supported | Iceberg |
|------|-----------|---------|
|Materialization: View|Yes| N/A |
|Materialization: Table|Yes| Yes |
|Materialization: Table with Partitions |Yes| Yes |
|Materialization: Incremental - Append|Yes| Yes |
|Materialization: Incremental - Append with Partitions |Yes| Yes |
|Materialization: Incremental - Insert+Overwrite |Yes| Yes |
|Materialization: Incremental - Insert+Overwrite with Partition |Yes| Yes |
|Materialization: Incremental - Merge|No| No |
|Materialization: Ephemeral|Yes| Yes |
|Seeds|Yes| Yes |
|Tests|Yes| Yes |
|Snapshots|No| No |
|Documentation|Yes| Yes |
|Authentication: LDAP|Yes| Yes |
|Authentication: Kerberos|Yes| No |

### Tests Coverage

#### Functional Tests
| Name | Base | Iceberg |
|------|------|---------|
|Materialization: View|Yes| N/A |
|Materialization: Table|Yes| Yes |
|Materialization: Table with Partitions |Yes| Yes |
|Materialization: Incremental - Append|Yes| Yes |
|Materialization: Incremental - Append with Partitions |Yes| Yes |
|Materialization: Incremental - Insert+Overwrite |Yes| Yes |
|Materialization: Incremental - Insert+Overwrite with Partition |Yes| Yes |
|Materialization: Ephemeral|Yes| Yes |
|Seeds|Yes| Yes |
|Tests|Yes| Yes |
|Snapshots|No| No |
|Documentation| Yes | Yes |
|Authentication: LDAP|Yes| Yes |
|Authentication: Kerberos|No| No |
