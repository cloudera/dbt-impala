# Kudu Integration using dbt-impala

The `dbt-impala` adapter allows you to use [dbt](https://www.getdbt.com/) along with [Apache Kudu](https://kudu.apache.org) and [Cloudera Data Platform](https://cloudera.com)


## Getting started

- [Install dbt](https://docs.getdbt.com/docs/installation)
- Read the [introduction](https://docs.getdbt.com/docs/introduction/) and [viewpoint](https://docs.getdbt.com/docs/about/viewpoint/)

### Requirements

- In a CDP public cloud deployment, Kudu is available as one of the many Cloudera Runtime services within the Real-time Data Mart template.
- To use Kudu, you can create a Data Hub cluster by selecting Real-time Data Mart template template in the Management Console.
- Follow this [article](https://blog.cloudera.com/integrating-cloudera-data-warehouse-with-kudu-clusters) on integrating the created Kudu service with Impala CDW.  


For general instructions, please follow [Readme](README.md) guidelines.

## Supported features
| Name | Supported | Kudu |
|------|-----------|---------|
|Materialization: View|Yes| N/A |
|Materialization: Table|Yes| Yes |
|Materialization: Table with Partitions |Yes| No |
|Materialization: Incremental - Append|Yes| Yes |
|Materialization: Incremental - Append with Partitions |Yes| No |
|Materialization: Incremental - Insert+Overwrite |Yes| No |
|Materialization: Incremental - Insert+Overwrite with Partition |Yes| No |
|Materialization: Incremental - Merge|No| No |
|Materialization: Ephemeral|Yes| No |
|Seeds|Yes| Yes |
|Tests|Yes| Yes |
|Snapshots|No| No |
|Documentation|Yes| Yes |
|Authentication: LDAP|Yes| Yes |
|Authentication: Kerberos|Yes| No |

### Tests Coverage

#### Functional Tests
| Name | Base | Kudu |
|------|------|---------|
|Materialization: View|Yes| N/A |
|Materialization: Table|Yes| Yes |
|Materialization: Table with Partitions |Yes| No |
|Materialization: Incremental - Append|Yes| Yes |
|Materialization: Incremental - Append with Partitions |Yes| No |
|Materialization: Incremental - Insert+Overwrite |Yes| Yes |
|Materialization: Incremental - Insert+Overwrite with Partition |Yes| No |
|Materialization: Ephemeral|Yes| Yes |
|Seeds|Yes| Yes |
|Tests|Yes| Yes |
|Snapshots|No| No |
|Documentation| Yes | Yes |
|Authentication: LDAP|Yes| Yes |
|Authentication: Kerberos|No| No |
