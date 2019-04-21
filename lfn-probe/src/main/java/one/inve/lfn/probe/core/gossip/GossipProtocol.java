package one.inve.lfn.probe.core.gossip;

import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.stream.Collectors;

import com.alibaba.fastjson.JSONObject;

import one.inve.bean.message.SnapshotMessage;
import one.inve.core.Config;
import one.inve.core.EventBody;
import one.inve.core.EventKeyPair;
import one.inve.rocksDB.RocksJavaUtil;
import one.inve.rpc.localfullnode.Event;
import one.inve.rpc.localfullnode.GossipObj;
import one.inve.service.SnapshotDbService;

/**
 * 
 * 
 * Copyright © CHXX Co.,Ltd. All rights reserved.
 * 
 * @Description: comparing lfn event height distance (gossip object)
 * @author: Francis.Deng
 * @date: Apr 21, 2019 6:29:27 AM
 * @version: V1.0
 */
public class GossipProtocol {

	private final ConcurrentHashMap<Integer, AtomicLongArray> lastSeq = new ConcurrentHashMap<>();
	private final String dbId;
	private final int shardId;

	public GossipProtocol(final int howManyshards, final int howManyNodesInOneShard, final int shardId,
			final String dbId) {
		this.dbId = dbId;
		this.shardId = shardId;

		for (int i = 0; i < howManyshards; i++) {
			AtomicLongArray lastSeqs = new AtomicLongArray(howManyNodesInOneShard);
			RocksJavaUtil rocksJavaUtil = new RocksJavaUtil(dbId);
			for (int j = 0; j < howManyNodesInOneShard; j++) {
				String key = i + "_" + j;
				byte[] value = rocksJavaUtil.get(key);
				if (null != value && value.length > 0) {
					lastSeqs.set(j, Integer.parseInt(new String(value)));
				} else {
					lastSeqs.set(j, -1);
				}
			}
			this.lastSeq.put(i, lastSeqs);
		}
	}

	/**
	 * mock up key subprocess of gossipMyMaxSeqList4Consensus(Local2localImpl) for
	 * diagnosis as well as getLastSeqsByShardId,getEventInMem(EventStoreImpl)
	 * 
	 * @param dbId
	 * @param snapVersion
	 * @param requesterSeqs
	 * @return
	 */
	public GossipObj getGossipObj(String snapVersion, long[] requesterSeqs) {
		GossipObj gossipObj = null;
		Instant first = Instant.now();
		SnapshotMessage snapshotMessage = SnapshotDbService.queryLatestSnapshotMessage(dbId);

		BigInteger currSnapshotVersion = snapshotMessage.getSnapVersion();

		if (currSnapshotVersion.equals(new BigInteger(snapVersion))
				|| currSnapshotVersion.subtract(BigInteger.ONE).equals(new BigInteger(snapVersion))
				|| currSnapshotVersion.add(BigInteger.ONE).equals(new BigInteger(snapVersion))) {

			List<Event> rpcEvents = getUnknownEvents(shardId, requesterSeqs).stream()
					.map(eventBody -> new Event(eventBody.getShardId(), eventBody.getCreatorId(),
							eventBody.getCreatorSeq(), eventBody.getOtherId(), eventBody.getOtherSeq(),
							eventBody.getTrans(), eventBody.getTimeCreated().getEpochSecond(),
							eventBody.getTimeCreated().getNano(), eventBody.getSignature(), eventBody.isFamous(),
							eventBody.getHash(), eventBody.getGeneration(),
							(null == eventBody.getConsTimestamp()) ? -1 : eventBody.getConsTimestamp().getEpochSecond(),
							(null == eventBody.getConsTimestamp()) ? -1 : eventBody.getConsTimestamp().getNano(),
							eventBody.getOtherHash(), eventBody.getParentHash()))
					.collect(Collectors.toList());

			gossipObj = (rpcEvents.size() > 0)
					? new GossipObj(currSnapshotVersion.toString(), rpcEvents.toArray(new Event[0]), null)
					: new GossipObj(currSnapshotVersion.toString(), null, null);

			long handleInterval = Duration.between(first, Instant.now()).toMillis();
			if (handleInterval > Config.DEFAULT_GOSSIP_EVENT_INTERVAL) {
				// logger.warn("----- gossipMyMaxSeqList4Consensus() {} interval: {} ms",
				// addressInfo, handleInterval);
			}
			return gossipObj;

		} else if (currSnapshotVersion.compareTo(new BigInteger(snapVersion)) > 0) {

			return new GossipObj(currSnapshotVersion.toString(), null,
					SnapshotDbService.querySnapshotMessageHashByVersion(dbId, snapVersion) == null ? null
							: SnapshotDbService.querySnapshotMessageHashByVersion(dbId, snapVersion).getBytes());
		} else {
			return new GossipObj(currSnapshotVersion.toString(), null, null);
		}
	}

	/**
	 * 获取片shardId内未知events
	 * 
	 * @param shardId     片号
	 * @param otherCounts 最大seq数组
	 * @return 未知events
	 */
	private ArrayList<EventBody> getUnknownEvents(int shardId, long[] otherCounts) {
		long[] currMyCounts = getLastSeqsByShardId(shardId);
		ArrayList<EventBody> diffEvents = new ArrayList<>();
		EventBody eventBody = null;

		for (int i = 0; i < currMyCounts.length; ++i) {
			for (long j = otherCounts[i] + 1L; j <= currMyCounts[i]; ++j) {
				eventBody = getEventInMem(shardId, (long) i, j);
				if (eventBody != null) {
					diffEvents.add(eventBody);
				}
			}
		}

		Collections.shuffle(diffEvents);
		if (diffEvents.size() > 1) {
			diffEvents.sort(Comparator.comparing(EventBody::getGeneration));
		}
//		logger.info("\n{} \ngetUnknownEvents(): requestor's seqs: {}, my seqs: {}, gossip event size = {}",
//				(null == connInfo ? null : connInfo.split("\\n")[1]), JSON.toJSONString(otherCounts),
//				JSON.toJSONString(currMyCounts), diffEvents.size());

		return cutResultUnknownEvents(diffEvents, Config.DEFAULT_SYNC_EVENT_COUNT);

	}

	protected long[] getLastSeqsByShardId(int shardId) {
		AtomicLongArray lastSeqs = this.lastSeq.get(shardId);
		int len = lastSeqs.length();
		long[] result = new long[len];

		for (int i = 0; i < len; ++i) {
			result[i] = lastSeqs.get(i);
		}
		return result;
	}

	protected EventBody getEventInMem(int shardId, long creatorId, long creatorSeq) {
		EventKeyPair pair = new EventKeyPair(shardId, creatorId, creatorSeq);
		EventBody eb = null;
		RocksJavaUtil rocksJavaUtil = new RocksJavaUtil(dbId);
		if (eb == null) {
//            if(logger.isDebugEnabled()) {
//                logger.debug("not in memory, query from database...");
//            }
			byte[] evt = rocksJavaUtil.get(pair.toString());
			if (null != evt && evt.length > 0) {
				eb = JSONObject.parseObject(new String(evt), EventBody.class);
			}
		}
		if (eb == null) {
			return null;
		}
		if (creatorSeq > 0) {
			EventKeyPair otherPair = new EventKeyPair(eb.getShardId(), eb.getOtherId(), eb.getOtherSeq());
			EventBody otherParent = null;
			if (otherParent != null) {
				eb.setOtherHash(otherParent.getHash());
			} else {
				byte[] evt = rocksJavaUtil.get(otherPair.toString());
				if (null != evt && evt.length > 0) {
					otherParent = JSONObject.parseObject(new String(evt), EventBody.class);
				}
				eb.setOtherHash(otherParent == null ? null : otherParent.getHash());
				if (otherParent == null) {
//					logger.error("(shardId, creatorId, creatorSeq)=({}, {}, {}) other parent is null", shardId,
//							creatorId, creatorSeq);
					System.err.println("(shardId, creatorId, creatorSeq)=({" + shardId + "}, {" + creatorId + "}, {"
							+ creatorSeq + "}) other parent is null");
				}
			}
			EventKeyPair selfPair = new EventKeyPair(eb.getShardId(), eb.getCreatorId(), eb.getCreatorSeq() - 1);
			EventBody selfParent = null;
			if (selfParent != null) {
				eb.setParentHash(selfParent.getHash());
			} else {
				byte[] evt = rocksJavaUtil.get(selfPair.toString());
				if (null != evt && evt.length > 0) {
					selfParent = JSONObject.parseObject(new String(evt), EventBody.class);
				}
				eb.setParentHash(selfParent == null ? null : selfParent.getHash());
				if (selfParent == null) {
//					logger.error("(shardId, creatorId, creatorSeq)=({}, {}, {}) self parent is null", shardId,
//							creatorId, creatorSeq);
					System.err.println("(shardId, creatorId, creatorSeq)=({" + shardId + "}, {" + creatorId + "}, {"
							+ creatorSeq + "}) other parent is null");
				}
			}
		}
		return eb;
	}

	/**
	 * 截取前面限定数量的event列表
	 * 
	 * @param events event列表
	 * @return 限定数量的event列表
	 */
	private ArrayList<EventBody> cutResultUnknownEvents(ArrayList<EventBody> events, long size) {
		if (events.size() > size) {
			// size += 1;
//Francis.Deng from Mar.25.2019
//issue of UST(unavailable submitted transaction): any submitted transaction could not been retrieved via consensus mechanism.

			ArrayList<EventBody> result = new ArrayList<>();
			for (int i = 0; i < size; i++) {
				EventBody eb = events.get(i);
				if (null != eb) {
					result.add(eb);
				}
			}
			return result;
		} else {
			return events;
		}
	}
}
