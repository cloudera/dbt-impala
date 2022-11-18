# dbt-impala

The `dbt-impala` adapter allows you to use [dbt](https://www.getdbt.com/) along with [Apache Impala](https://impala.apache.org/) and [Cloudera Data Platform](https://cloudera.com)


## Getting started

- [Install dbt](https://docs.getdbt.com/docs/installation)
- Read the [introduction](https://docs.getdbt.com/docs/introduction/) and [viewpoint](https://docs.getdbt.com/docs/about/viewpoint/)

### Requirements

Python >= 3.8

dbt-core ~= 1.3.0

impyla >= 0.18

python-decouple >= 3.6

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
| Name | Supported |
|------|-----------|
|Materialization: Table|Yes|
|Materialization: View|Yes|
|Materialization: Incremental - Append|Yes|
|Materialization: Incremental - Insert+Overwrite|Yes|
|Materialization: Incremental - Merge|No|
|Materialization: Ephemeral|No|
|Seeds|Yes|
|Tests|Yes|
|Snapshots|Yes|
|Documentation|Yes|
|Authentication: LDAP|Yes|
|Authentication: Kerberos|Yes|
