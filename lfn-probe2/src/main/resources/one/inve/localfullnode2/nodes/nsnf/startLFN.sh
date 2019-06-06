
#!/bin/bash


CLASSPATH=localfullnode2-2.0.0.jar

MAINCLASS=one.inve.localfullnode2.nodes.Localfullnode2

for ((index=0;index<=2;index++))
do
	java -cp $CLASSPATH $MAINCLASS -Dio.netty.leakDetectiontionLevel=advanced --Ice.Config=${index}default.config -Ddatabase.reset=false -Dtest.prefix=${index} &
done
