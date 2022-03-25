# dbt-impala

The `dbt-impala` adapter allows you to use dbt along with [Apache Impala](https://impala.apache.org/) and [Cloudera Data Platform](https://cloudera.com)


## Getting started

- [Install dbt](https://docs.getdbt.com/docs/installation)
- Read the [introduction](https://docs.getdbt.com/docs/introduction/) and [viewpoint](https://docs.getdbt.com/docs/about/viewpoint/)


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
|Materialization: Incremental|No|
|Materialization: Ephemeral|No|
|Seeds|Yes|
|Tests|No|
|Snapshots|No|
|Documentation|No|
|Authentication: LDAP|Yes|
|Authentication: Kerberos|No|
