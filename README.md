# dbt-impala

The `dbt-impala` adapter allows you to use [dbt](https://www.getdbt.com/) along with [Apache Impala](https://impala.apache.org/) and [Cloudera Data Platform](https://cloudera.com)


## Getting started

- [Install dbt](https://docs.getdbt.com/docs/installation)
- Read the [introduction](https://docs.getdbt.com/docs/introduction/) and [viewpoint](https://docs.getdbt.com/docs/about/viewpoint/)

### Requirements

Current version of dbt-impala work only with dbt-core 1.3 but not with dbt-core >= 1.4.
We are actively working on next release 1.4 which will work with dbt-core 1.4

Python >= 3.8
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

### Tests

To locally test the adapter and test it against a given CDH cluster. We have the integration-tests suites written inside the tests directory. 

To install the tests dependencies:
```
pip install -r dev_requirements.txt
```

To use the local version dbt-impala
```
pip install -e .
```

#### Testing Framework:

To test the dbt adaptor, we are using the dbt-tests-adapter module which has predefined set of tests and test-data (fixtures). More details [how to test dbt-adapter](https://docs.getdbt.com/guides/dbt-ecosystem/adapter-development/4-testing-a-new-adapter)

To run the tests against impala instance (as insecure):
```
IMPALA_HOST=<host> python3 -m pytest --profile cdh_endpoint
```

To run the tests against impala instance (via ldap)
```
IMPALA_HOST=<host> IMPALA_SCHEMA=<schema> IMPALA_USER=<user> IMPALA_PASSWORD=<password> python3 -m pytest --profile dwx_endpoint
```