#!/usr/bin/env bash

curl -u hduser:123456789aA. -d user.name=hduser https://hd-cluster.azurehdinsight.net/templeton/v1/jobs/job_1531816878813_0025

result:
{"status":{"mapProgress":1.0,"reduceProgress":1.0,"cleanupProgress":0.0,"setupProgress":0.0,"runState":2,"startTime":1531830735095,"queue":"default","priority":"NORMAL","schedulingInfo":"NA","failureInfo":"NA","jobACLs":{},"jobName":"TempletonControllerJob","jobFile":"wasb://hd-cluster@hdstoragetest.blob.core.windows.net/mr-history/done/2018/07/17/000000/job_1531816878813_0025_conf.xml","finishTime":1531830787602,"historyFile":"","trackingUrl":"hn1-hd-clu.5qf52yhk3jqedalb11kxd4coud.ax.internal.cloudapp.net:19888/jobhistory/job/job_1531816878813_0025","numUsedSlots":0,"numReservedSlots":0,"usedMem":0,"reservedMem":0,"neededMem":0,"jobId":"job_1531816878813_0025","username":"hduser","jobPriority":"NORMAL","jobID":{"id":25,"jtIdentifier":"1531816878813"},"jobComplete":true,"retired":false,"uber":false,"state":"SUCCEEDED"},"profile":{"user":"hduser","jobFile":"wasb://hd-cluster@hdstoragetest.blob.core.windows.net/mr-history/done/2018/07/17/000000/job_1531816878813_0025_conf.xml","url":null,"queueName":"default","jobId":"job_1531816878813_0025","jobName":"TempletonControllerJob","jobID":{"id":25,"jtIdentifier":"1531816878813"}},"id":"job_1531816878813_0025","parentId":null,"percentComplete":null,"exitValue":0,"user":"hduser","callback":null,"completed":"done","userargs":{"statusdir":"wasb:///example/curl","file":"wasb:///example/hivequery2.sql","enablejobreconnect":null,"define":[],"enablelog":"false","user.name":"hduser","files":null,"callback":null,"execute":null},"msg":null}%


curl -u hduser:123456789aA. -d user.name=hduser -d file="wasb:///example/hivequery2.sql" -d statusdir="wasb:///example/curl" https://hd-cluster.azurehdinsight.net/templeton/v1/hive

result:
{"id":"job_1531816878813_0028"}%

curl -X POST \
  https://hd-cluster.azurehdinsight.net/livy/batches \
  -H 'Authorization: Basic aGR1c2VyOjEyMzQ1Njc4OWFBLg==' \
  -H 'Content-Type: application/json' \
  -H 'X-Requested-By: user' \
  -d '{
	"name":"spark-task-1",
	"executorMemory": "1g",
	"className":"org.alikemal.spark.examples.SparkPi",
	"jars":["/jars/calculatepi-1.0-SNAPSHOT-dep.jar"],
	"file":"/jars/calculatepi-1.0-SNAPSHOT-dep.jar"
}'

result
{"id":6,"state":"starting","appId":null,"appInfo":{"driverLogUrl":null,"sparkUiUrl":null},"log":["stdout: ","\nstderr: ","\nYARN Diagnostics: "]}%

curl -X GET   'https://hd-cluster.azurehdinsight.net/livy/batches/6'  -H 'Authorization: Basic aGR1c2VyOjEyMzQ1Njc4OWFBLg==' -H 'Content-Type: application/json' -H 'X-Requested-By: user'

result 
{"id":6,"state":"success","appId":"application_1531816878813_0027","appInfo":{"driverLogUrl":null,"sparkUiUrl":"https://hd-cluster.azurehdinsight.net/yarnui/hn/proxy/application_1531816878813_0027/"},"log":["\t queue: default","\t start time: 1531832699181","\t final status: UNDEFINED","\t tracking URL: https://hd-cluster.azurehdinsight.net/yarnui/hn/proxy/application_1531816878813_0027/","\t user: livy","18/07/17 13:04:59 INFO ShutdownHookManager: Shutdown hook called","18/07/17 13:04:59 INFO ShutdownHookManager: Deleting directory /tmp/spark-b95487be-34ea-4a97-b407-83459c5494ea","18/07/17 13:04:59 INFO ShutdownHookManager: Deleting directory /tmp/spark-0ff99ce0-5be5-40b0-871b-7ed1a99df290","\nstderr: ","\nYARN Diagnostics: "]}% 

