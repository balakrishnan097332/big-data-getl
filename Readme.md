# Big Data Spark GETL

Upgrading smart system lift to the new Generic Extract Transform and Load pattern

## Development setup

### Prerequisites

- Apache Spark is "installed" and and the environment variable SPARK_HOME has been set.
- [virtualenvwrapper](https://virtualenvwrapper.readthedocs.io/en/latest/install.html) (or [virtualenv](https://virtualenv.readthedocs.org/en/latest/installation.html)) has been installed.
- [Visual studio code](https://code.visualstudio.com/download) or any python editor

### Install python packages

Create a virtual environment for big-data-getl (with python 3.5) and update pip to the latest version:

```
$ mkvirtualenv -p /usr/bin/python3.5 big-data-getl
$ pip install --upgrade pip
```

To install all dependenvies from the Pipfile,

```
$ pipenv install
```

## Running the tests

To run all the tests you can use the script bin/run-tests.sh

```
$ ./bin/run_tests.sh
```

## Modules

### Load

- Accepts SparkSession, list of JSON files, schema and returns a Dataframe on a successful load
- Accepts SparkSession, list of XML files, schema, tag name which is to be considered as root and returns a Dataframe on a successful load
- Accepts SparkSession and list of files of any kind and returns an RDD
