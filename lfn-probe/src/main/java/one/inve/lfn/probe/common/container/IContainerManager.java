package one.inve.lfn.probe.common.container;

import java.util.Queue;

import one.inve.core.EventBody;

public interface IContainerManager {
	Queue<EventBody> getShardSortQueue(int shardingNum);
}
