# Changelog

## 1.3.2 (May 10th, 2023)
Added support for iceberg table format
Added support for plain authorization connection
Added support for Python 3.7
Added contribution guideline for developers
Added capability to run integration tests against an endpoint
Added set of integration tests to cover major use-cases
Bug fixes:
1. Adaptor should return empty list if table doesn't exist rather than throwing error
2. Fixed the partition by clause queries ie remove duplicate columns
3. Fixed the increment behavior with partition, ie to use partitions during the increment append
4. Fixed the incremental insert queries syntax, to support iceberg format
3. Fixed broken integration tests

## 1.3.0 (Nov 18th, 2022)
Added dbt-core 1.3.0 support

## 1.2.0 (Nov 2nd, 2022)
Added dbt-core 1.2.0 support

## 1.1.5 (Oct 28th, 2022)
Updated instrumentation schema

## 1.1.4 (Sep 30th, 2022)
Added user-agent string to improve instrumentation. dbt-impala also adds better error handling and display to the user.

## 1.1.3 (Sep 17th, 2022)
Adding support for append mode when partition_by clause is used. Along with an updated instrumentation package.

## 1.1.2 (Aug 5th, 2022)  
Now dbname in profile.yml file is optional; Updated a dependency in README; dbt-core version updates automatically in setup.py

## 1.1.1 (Jul 16th, 2022)  
Bug fixes for a specific function

## 1.1.0 (Jun 9th, 2022)  
Adapter migration to dbt-core-1.1.0; added time-out for snowplow endpoint to handle air-gapped env  

## 1.0.6 (May 23rd, 2022)  
Added support to insert_overwrite mode for incremental models and added instrumentation to the adapter

## 1.0.5 (Apr 29th, 2022)  
Added support to an EXTERNAL clause with table materialization & improved error handling for relation macros  

## 1.0.4 (Apr 1st, 2022)  
Added support to Kerberos authentication method and dbt-docs. Previous two releases were internal.

## 1.0.1 (Mar 25th, 2022)  
Cloudera released the first version of the dbt-impala adapter
