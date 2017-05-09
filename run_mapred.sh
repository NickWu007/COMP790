#!/bin/bash
hadoop fs -rm -r /user/junaowu/wiki/output
./mapred_javac.sh WikiHourCount.java
./mapred_jar.sh WikiHourCount.jar
hadoop jar WikiHourCount.jar WikiHourCount /user/junaowu/wiki/input /user/junaowu/wiki/output
hadoop fs -getmerge /user/junaowu/wiki/output results.txt
sort -nr -k 2 results.txt > sorted.txt