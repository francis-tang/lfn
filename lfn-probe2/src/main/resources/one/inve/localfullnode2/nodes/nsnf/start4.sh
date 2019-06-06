#!/bin/bash


CLASSPATH=localfullnode2-2.0.0.jar
CLASSPATH=$CLASSPATH:lfn-probe2-0.7.0.jar

MAINCLASS=one.inve.localfullnode2.nodes.nsnf.ConfigurableLocalfullnodes

for ((index=0;index<=3;index++))
do
	java -cp $CLASSPATH $MAINCLASS -Dio.netty.leakDetectiontionLevel=advanced --Ice.Config=${index}default.config -Ddatabase.reset=false -Dtest.prefix=${index} -Dnsnf.conf=4${index}.nsnf.toml &
done
