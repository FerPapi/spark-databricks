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
1.	Install dependencies (pip/anaconda) 

`pip install -r requirements.txt`

2.	Clone our Tardis repo and make it available as a package 

`git clone https://transferoswiss@dev.azure.com/transferoswiss/Propdesk/_git/propdesk_tardis`

3. Clone the Azure Services repo and make it available as a package

`git clone https://transferoswiss@dev.azure.com/transferoswiss/Propdesk/_git/propdesk_azure_services`

# Running tests
`python -m tests.estimators_unittest`