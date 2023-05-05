# Contributing to `dbt-impala`

1. [About this document](#about-this-document)
2. [Getting the code](#getting-the-code)
3. [Running `dbt-impala` in development](#running-dbt-impala-in-development)
4. [Testing](#testing)
4. [Submitting a Pull Request](#submitting-a-pull-request)

## About this document
This document is a guide for anyone interested in contributing to the `dbt-impala` repository. It outlines how to create issues and submit pull requests (PRs).

This is not intended as a guide for using `dbt-impala` in a project.

We assume users have a Linux or MacOS system. You should have familiarity with:

- Python `virturalenv`s
- Python modules
- `pip`
- common command line utilities like `git`.

In addition to this guide, we highly encourage you to read the [dbt-core](https://github.com/dbt-labs/dbt-core/blob/main/CONTRIBUTING.md). Almost all information there is applicable here!

## Getting the code

 `git` is needed in order to download and modify the `dbt-impala` code. There are several ways to install Git. For MacOS, we suggest installing [Xcode](https://developer.apple.com/support/xcode/) or [Xcode Command Line Tools](https://mac.install.guide/commandlinetools/index.html).

### External contributors

If you are not a member of the `Cloudera` GitHub organization, you can contribute to `dbt-impala` by forking the `dbt-impala` repository. For more on forking, check out the [GitHub docs on forking](https://help.github.com/en/articles/fork-a-repo). In short, you will need to:

1. fork the `dbt-impala` repository
2. clone your fork locally
3. check out a new branch for your proposed changes
4. push changes to your fork
5. open a pull request of your forked repository against `cloudera/dbt-impala`

### Cloudera contributors

If you are a member of the `Cloudera` GitHub organization, you will have push access to the `dbt-impala` repo. Rather than forking `dbt-impala` to make your changes, clone the repository like normal, and check out feature branches.

## Running `dbt-impala` in development

### Installation

1. Ensure you have the latest version of `pip` installed by running `pip install --upgrade pip` in terminal.

2. Configure and activate a `virtualenv` as described in [Setting up an environment](https://github.com/dbt-labs/dbt-core/blob/HEAD/CONTRIBUTING.md#setting-up-an-environment).

3. Install `dbt-core` in the active `virtualenv`. To confirm you installed dbt correctly, run `dbt --version` and `which dbt`.

4. Install `dbt-impala` and development dependencies in the active `virtualenv`. Run `pip install -e . -r dev-requirements.txt`.

5. Add the pre-commit hook. Run `pre-commit install`

When `dbt-impala` is installed this way, any changes you make to the `dbt-impala` source code will be reflected immediately (i.e. in your next local dbt invocation against a Impala target).

## Testing

### Initial setup

`dbt-impala` contains [functional](https://github.com/cloudera/dbt-impala/tree/master/tests/functional/) tests. Functional tests require an actual Impala warehouse to test against.

- You can run functional tests "locally" by configuring a `test.env` file with appropriate `ENV` variables.

```
cp test.env.example test.env
$EDITOR test.env
```

WARNING: The parameters in your `test.env` file must link to a valid Impala instance. The `test.env` file you create is git-ignored, but please be _extra_ careful to never check in credentials or other sensitive information when developing.


### "Local" test commands
There are a few methods for running tests locally.

#### `pytest`
You may run a specific test or group of tests using `pytest` directly. Activate a Python virtualenv active with dev dependencies installed. Use the appropriate profile like cdh_endpoint or dwx_endpoint. Then, run tests like so:

```sh
# Note: replace $strings with valid names

# run full tests suite against an environment/endpoint
python -m pytest --profile dwx_endpoint

# run all impala functional tests in a directory
python -m pytest tests/functional/$test_directory --profile dwx_endpoint
python -m pytest tests/functional/adapter/test_basic.py --profile dwx_endpoint

# run all impala functional tests in a module
python -m pytest --profile dwx_endpoint tests/functional/$test_dir_and_filename.py
python -m pytest --profile dwx_endpoint tests/functional/adapter/test_basic.py

# run all impala functional tests in a class
python -m pytest --profile dwx_endpoint tests/functional/$test_dir_and_filename.py::$test_class_name
python -m pytest --profile dwx_endpoint tests/functional/adapter/test_basic.py::TestSimpleMaterializationsImpala

# run a specific impala functional test
python -m pytest --profile dwx_endpoint tests/functional/$test_dir_and_filename.py::$test_class_name::$test__method_name
python -m pytest --profile dwx_endpoint tests/functional/adapter/test_basic.py::TestSimpleMaterializationsImpala::test_base
```

To configure the pytest setting, update pytest.ini. By default, all the tests run logs are captured in `logs/<test-run>/dbt.log`

## Submitting a Pull Request

A `dbt-impala` maintainer will review your PR and will determine if it has passed regression tests. They may suggest code revisions for style and clarity, or they may request that you add unit or functional tests. These are good things! We believe that, with a little bit of help, anyone can contribute high-quality code.

Once all tests are passing and your PR has been approved, a `dbt-impala` maintainer will merge your changes into the active development branch. And that's it! Happy developing :tada:
