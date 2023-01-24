# Introduction
In this repository we have production level code for our pipeline of data. It includes tests and examples.

Pipeline basically goes as follows:

Setup:
```
Have a pyspark code ready for production
  - put it in spark_jobs/ with a name as spark_<filename>.py
  - run databricks_deploy.py
```

Pipeline normally goes as follows:
```
  - trigger a pyspark execution on databricks
  - script on datatricks gets data from a data source (such as Tardis)
  - script makes calculations on data, ideally based on dostributed computing framework
  - script uploads data to a sink (a database or data lake such as Azure Storage) 
```

# Getting Started

This is a demo repo that depends on many other internal libs

# Running tests
`python -m tests.estimators_unittest`
