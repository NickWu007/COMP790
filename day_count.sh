#!/bin/bash
# Import data
hadoop fs -rm -r /user/junaowu/wiki/input
hadoop fs -mkdir /user/junaowu/wiki/input
for i in `seq 0 23`;
do
	for j in `seq 0 13`;
	do
		hms=""
		if [ "$i" -lt "11" ];
		then
			if [ "$j" -lt "11" ];
			then
				hms=0"$i"000"$j"
			else
				hms=0"$i"00"$j"
			fi
		else
			if [ "$j" -lt "11" ];
			then
				hms="$i"000"$j"
			else
				hms="$i"00"$j"
			fi
		fi
		wget https://dumps.wikimedia.org/other/pagecounts-raw/$1/$1-$2/pagecounts-$1$2$3-$hms.gz
		gunzip pagecounts-$1$2$3-$hms.gz 
		hadoop fs -put pagecounts-$1$2$3-$hms /user/junaowu/wiki/input
		rm pagecounts-$1$2$3-$hms
	done
done

#run Mapreduce
hadoop fs -rm -r /user/junaowu/wiki/output
./mapred_javac.sh WikiHourCount.java
./mapred_jar.sh WikiHourCount.jar
hadoop jar WikiHourCount.jar WikiHourCount /user/junaowu/wiki/input /user/junaowu/wiki/output

#gather results
hadoop fs -getmerge /user/junaowu/wiki/output results.txt
sort -nr -k 2 results.txt > sorted_$1$2$3.txt