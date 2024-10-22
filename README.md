# dbt-impala

The `dbt-impala` adapter allows you to use [dbt](https://www.getdbt.com/) along with [Apache Impala](https://impala.apache.org/) and [Cloudera Data Platform](https://cloudera.com)


## Getting started

- [Install dbt](https://docs.getdbt.com/docs/installation)
- Read the [introduction](https://docs.getdbt.com/docs/introduction/) and [viewpoint](https://docs.getdbt.com/docs/about/viewpoint/)
- For using `dbt-impala` adapter against [Apache Kudu](https://kudu.apache.org), please follow [Kudu Integration](KUDU_INTEGRATION.md) guidelines.

### Requirements

Current version of dbt-impala uses dbt-core 1.8.*. We are actively working on supporting the next available version of dbt-core.

Python >= 3.7
dbt-core == 1.8.*

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
| Name | Supported | Iceberg | Kudu |
|------|-----------|---------|------|
|Materialization: View|Yes| N/A | N/A |
|Materialization: Table|Yes| Yes | Yes |
|Materialization: Table with Partitions |Yes| Yes | No |
|Materialization: Incremental - Append|Yes| Yes | Yes |
|Materialization: Incremental - Append with Partitions |Yes| Yes | No |
|Materialization: Incremental - Insert+Overwrite |Yes| Yes | Yes |
|Materialization: Incremental - Insert+Overwrite with Partition |Yes| Yes | No |
|Materialization: Incremental - Merge|No| No | No |
|Materialization: Ephemeral|Yes| Yes | No |
|Seeds|Yes| Yes | Yes |
|Tests|Yes| Yes | Yes |
|Snapshots|No| No | No |
|Documentation|Yes| Yes | Yes |
|Authentication: LDAP|Yes| Yes | Yes |
|Authentication: Kerberos|Yes| No | No |

### Tests Coverage

#### Functional Tests
| Name | Base | Iceberg | Kudu |
|------|------|---------|------|
|Materialization: View|Yes| N/A | N/A |
|Materialization: Table|Yes| Yes | Yes |
|Materialization: Table with Partitions |Yes| Yes | No |
|Materialization: Incremental - Append|Yes| Yes | Yes |
|Materialization: Incremental - Append with Partitions |Yes| Yes | No |
|Materialization: Incremental - Insert+Overwrite |Yes| No | No |
|Materialization: Incremental - Insert+Overwrite with Partition |Yes| Yes | No |
|Materialization: Ephemeral|Yes| Yes | No |
|Seeds|Yes| Yes | Yes |
|Tests|Yes| Yes | Yes |
|Snapshots|No| No | No |
|Documentation| Yes | Yes | Yes |
|Authentication: LDAP|Yes| Yes | Yes |
|Authentication: Kerberos|No| No | No |
