## Sales recommendations
A small microservice to find goods that sell together.
1. External module in 1C produces export data about sales in csv-files and uploads it in a filestorer service
2. ETL-process takes the file from filestorer, then extracts the archive of csv-files and imports it into the postgresql database.
3. The microservice starts processing data from the database. [FP-Max algorithm from mlxtend](https://rasbt.github.io/mlxtend/user_guide/frequent_patterns/fpmax/) python library effectively finds itemsets
4. Then put the results as a new archive into the filestorer service
5. The external module in 1C download results and use them to show recommendations for sellers

All steps orchestrate by [Prefect](https://docs.prefect.io/latest/).
Github repo works like code place.
Docker uses like infrastructure to run the code.