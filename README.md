# Project Description
Sparkify has decided to introduce automation and monitoring to their data warehouse ETL pipelines. They want dynamic pipeline that can be build reusable tasks, monitor, and allow easy backfills. They also want to run quality tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

# Datasets
This project uses two datasets which are available at:

* Log data: s3://udacity-dend/log_data
* Song data: s3://udacity-dend/song_data

# Project Overview
The project uses the core concepts of Apache Airflow using custom operators to perform tasks such as staging the data, filling the data warehouse in Amazon Redshift, and running checks on the data as the final step. It also contains helper class that contains all the SQL transformations. 

The project contains three major components:

* Dag folder that imports the tasks and the task dependencies have been set
* Operators folder with custom operators
* Helper class for the SQL transformations
* DAG in the Airflow UI.

DAG has been configured with these default parameters:

* The DAG does not have dependencies on past runs
* On failure, the task are retried 3 times
* Retries happen every 5 minutes
* Catchup is turned off
* Do not email on retry

## Notes about perators and task instances that run SQL statements against the Redshift database. 

### Stage Operator
The stage operator is able to load any JSON formatted files from S3 to Amazon Redshift. The operator creates and runs a SQL COPY statement based on the parameters provided. The operator's parameters specify where in S3 the file is loaded and what is the target table.

The parameters used are able to distinguish between JSON file. It Another important requirement of the stage operator is containing a templated field that allows it to load timestamped files from S3 based on the execution time and run backfills.

### Fact and Dimension Operators
Dimension and fact operators utilize the provided SQL helper class to run data transformations. Most of the logic is within the SQL transformations and the operator is expected to take as input a SQL statement and target database on which to run the query against. 

Dimension loads are often done with the truncate-insert pattern where the target table is emptied before the load. Thus, a parameter that allows switching between insert modes when loading dimensions. Fact tables are usually so massive that they should only allow append type functionality.

### Data Quality Operator
The final operator to create is the data quality operator, which is used to run checks on the data itself. The operator's main functionality is to receive one or more SQL based test cases along with the expected results and execute the tests. For each the test, the test result and expected result needs to be checked and if there is no match, the operator should raise an exception and the task should retry and fail eventually.

For example one test is a SQL statement that checks if certain column contains NULL values by counting all the rows that have NULL in the column. We do not want to have any NULLs so expected result would be 0 and the test would compare the SQL statement's outcome to the expected result.

### Subdag Operator
The operator combines SQL statements for creating and inserting into dimension tables

## Add Airflow Connections
Here, we'll use Airflow's UI to configure your AWS credentials and connection to Redshift.

### To go to the Airflow UI:
You can use the Project Workspace here and click on the blue Access Airflow button in the bottom right.
If you'd prefer to run Airflow locally, open http://localhost:8080 in Google Chrome (other browsers occasionally have issues rendering the Airflow UI).
1. Click on the Admin tab and select Connections
![alt text](https://github.com/piushvaish/airflow-data-pipelines-solution/blob/master/airflow_images/admin-connections.png, "admin-connections")

2. Under Connections, select Create.
![alt text](airflow_images/create-connection.PNG, "create-connection")
3. On the create connection page, enter the following values:

* Conn Id: Enter aws_credentials.
* Conn Type: Enter Amazon Web Services.
* Login: Enter your Access key ID from the IAM User credentials you downloaded earlier.
* Password: Enter your Secret access key from the IAM User credentials you downloaded earlier.
Once you've entered these values, select Save and Add Another.

![connection-aws](airflow_images/connection-aws-credentials.PNG, "connection-aws-credentials")

4. On the next create connection page, enter the following values:

* Conn Id: Enter redshift.
* Conn Type: Enter Postgres.
* Host: Enter the endpoint of your Redshift cluster, excluding the port at the end. You can find this by selecting your cluster in the Clusters page of the Amazon Redshift console. See where this is located in the screenshot below. IMPORTANT: Make sure to NOT include the port at the end of the Redshift endpoint string.
* Schema: Enter dev. This is the Redshift database you want to connect to.
* Login: Enter awsuser.
* Password: Enter the password you created when launching your Redshift cluster.
* Port: Enter 5439.
Once you've entered these values, select Save.
![alt text](airflow_images/cluster-details.PNG, "cluster-details")
You are all configured to run Airflow with Redshift.
![alt text](airflow_images/connection-redshift.PNG, "connection-redshift")

## References
* https://hub.udacity.com/rooms/community:nd027:843753-project-565-smg-2?contextType=room
* https://airflow.apache.org/docs/stable/#content
* https://github.com/danieldiamond
* https://github.com/DrakeData
* https://github.com/Flor91
* https://www.polidea.com/blog/apache-airflow-tutorial-and-beginners-guide/





