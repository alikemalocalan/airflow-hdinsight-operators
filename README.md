# airflow-hdinsight-operators
Azure HDInsight Operators For Apache Airflow

Pre-run Steps:
1. Open the Airflow WebServer
2. Navigate to Admin -> Connections
3. Add a new connection
    1. Set the Conn Id as "azure_http_conn"
    2. Set the Conn Type as "http"
    3. Set the host
    4. Set the port (default for azure-rest-api is 80)
    5. Save


### TODO list

- [x] Hive SQL file Submit operator
- [x] Spark Submit operator
- [ ] Create cluster
- [ ] Delete Cluster
