#!/usr/bin/env bash
PWD=`pwd`
export HADOOP_CLASSPATH=access-log-analyzer-1.0-SNAPSHOT.jar
hadoop io.snows.acl.driver.PvDriver -conf conf/hadoop-local.xml \
      file://$PWD/data/ file://$PWD/out1/ file://$PWD/out2/