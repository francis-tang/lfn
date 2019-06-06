


java -jar -Dio.netty.leakDetectiontionLevel=advanced seed-2.0.0.jar --Ice.Config=s.default.config -Dshard.size=1 -Dshard.node.size=4 -Dsharding.static=1  &

java -jar -Dio.netty.leakDetectiontionLevel=advanced fullnode-2.0.0.jar --Ice.Config=f.0default.config -Dtest.prefix=0 -Dshard.size=1 -Dshard.node.size=4 -Dsharding.static=1  &

java -jar -Dio.netty.leakDetectiontionLevel=advanced fullnode-2.0.0.jar --Ice.Config=f.1default.config -Dtest.prefix=1 -Dshard.size=1 -Dshard.node.size=4 -Dsharding.static=1  & 

java -jar -Dio.netty.leakDetectiontionLevel=advanced fullnode-2.0.0.jar --Ice.Config=f.2default.config -Dtest.prefix=2 -Dshard.size=1 -Dshard.node.size=4 -Dsharding.static=1  &  
