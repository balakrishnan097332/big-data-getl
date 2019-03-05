#!/bin/sh -e

# Testing the data getl library
# Making test Directories
mkdir -p test-reports
rm -rf test-reports/* 2>/dev/null

mkdir -p coverage-reports
rm -rf coverage-reports/* 2>/dev/null

pipenv install --dev
pipenv run pytest --cov big_data_getl --cov-report=xml:./coverage-reports/coverage.xml --junitxml=./test-reports/junit_coverage.xml --cov-report term
sed -i "s#/home/ubuntu/big/big_data_getl#$BAMBOOWD/big_data_getl#g" ./coverage-reports/coverage.xml >> ./coverage-reports/coverage.xml

# Generate pylint report
pipenv run pylint big_data_getl -r n --msg-template="{path}:{line}: [{msg_id}({symbol}), {obj}] {msg}" | grep "^[a-zA-Z0-9]" >> ./test-reports/pylint-report.txt
chmod 755 -R coverage-reports/
chmod 755 -R test-reports/
