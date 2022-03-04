Getting this running locally:

Requirements:
python3.8 or higher
dbt-core (pip3 install dbt-core)
dbt-adapter-tests (pip3 install pytest-dbt-adapter)
impyla (https://github.com/cloudera/impyla) 
impala docker instance (https://github.com/TickSmith/impala-docker)

Edit:
$HOME/.dbt/profile.xml to set correct profile for conecting to impala instance

To Install the Adapter:
python3 setup.py install

Simple dbt test:
dbt debug (this will ceck connection, and see if it can execute a simple static SQL query)
dbt run (this will generate a materialized table, and a dependent view)

Other tests:
This is based on dbt-adapter-test, and individual test spec files are added in the test/ directory.

To run a test:
pytest test/[spec-file]

