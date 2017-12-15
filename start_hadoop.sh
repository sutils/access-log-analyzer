#!/usr/bin/env bash
cd $HADOOP_HOME
export HADOOP_CLASSPATH=share/hadoop/yarn/test/hadoop-yarn-server-tests-2.9.0-tests.jar:share/hadoop/yarn/timelineservice/hadoop-yarn-server-timelineservice-2.9.0.jar
bin/hadoop jar ./share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-2.9.0-tests.jar minicluster -writeConfig /tmp/hadoop.xml