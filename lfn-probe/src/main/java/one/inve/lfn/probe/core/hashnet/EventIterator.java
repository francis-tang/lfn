package one.inve.lfn.probe.core.hashnet;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.alibaba.fastjson.JSONObject;

import one.inve.core.Config;
import one.inve.core.EventBody;
import one.inve.core.EventKeyPair;
import one.inve.rocksDB.RocksJavaUtil;

public class EventIterator<E> implements Iterator<E> {
	int shardId;
	int selfId;
	int n;

	BigInteger lastSeqs[];

	int index = 0;
	int page = 0;
	int sizePerHashnetNode = 0;
	int sizePerCycle = 0;
	int num = 0;
	String dbId = null;
	boolean flag = false;
	List<EventBody> eventBodys = null;

	EventIterator(int shardId, int selfId, int n, String dbId) {
		this.selfId = selfId;
		this.shardId = shardId;
		this.n = n;
		this.dbId = dbId;

		// 获取和计算lastSeq
		this.getlastSeqs();

		// 分片shardId的hashnet的每个柱子的一轮读取最大数量
		this.sizePerHashnetNode = Config.READ_SIZE_FROM_DB_PER_HASHNETNODE;
		// 一轮读取Event的最大数量
		this.sizePerCycle = this.sizePerHashnetNode * this.n;

		// 读取一轮Event
		eventBodys = this.getAllEvent4DB(page);
		num = eventBodys.size();
//        if(num == sizePerCycle) {
//            flag = true;
//        } else {
//            logger.warn("node-({}, {}): num = {}, sizePerCycle-{}",
//                    this.shardId, this.selfId, num, sizePerCycle);
//        }
	}

	@Override
	public boolean hasNext() {
//        logger.info("node-({}, {}): hasNext(), index: {}, num: {}, flag: {}", this.shardId, this.selfId, index, num, flag);
		if (flag && index == num) {
			getNewEventBodys();
		}
		return index < num;
	}

	@Override
	public E next() {
//        logger.info("node-({}, {}): next(), index: {}, num: {}, flag: {}", this.shardId, this.selfId, index, num, flag);
		if (flag && index == num) {
			getNewEventBodys();
		}
		if (index < num) {
			return (E) eventBodys.get(index++);
		} else {
			index++;
			return null;
		}
	}

	private void getlastSeqs() {
		lastSeqs = new BigInteger[n];
		RocksJavaUtil rocksJavaUtil = new RocksJavaUtil(this.dbId);
		for (int j = 0; j < n; j++) {
			byte[] seqByte = rocksJavaUtil.get(this.shardId + "_" + j);
			if (null != seqByte && seqByte.length > 0) {
				lastSeqs[j] = new BigInteger(new String(seqByte));

				BigInteger lastSeq = lastSeqs[j];
				EventKeyPair pair = new EventKeyPair(this.shardId, j, lastSeq.longValue());
				byte[] ebByte = rocksJavaUtil.get(pair.toString());
				if (null == ebByte || ebByte.length <= 0) {
					// 向前获取存在的Event
					while (null == ebByte || ebByte.length <= 0) {
						lastSeq = lastSeq.subtract(BigInteger.ONE);
						if (lastSeq.equals(BigInteger.ZERO)) {
							break;
						}
						pair = new EventKeyPair(this.shardId, j, lastSeq.longValue());
						ebByte = rocksJavaUtil.get(pair.toString());
					}
				} else {
					// 向后获取更新的Event
					while (null != ebByte && ebByte.length > 0) {
						lastSeq = lastSeq.add(BigInteger.ONE);

						pair = new EventKeyPair(this.shardId, j, lastSeq.longValue());
						ebByte = rocksJavaUtil.get(pair.toString());
					}
					lastSeq = lastSeq.subtract(BigInteger.ONE);
				}
				if (!lastSeqs[j].equals(lastSeq)) {
//					logger.warn("node-({}, {}): ({}, {}) lastSeq diff: db-{}, calcu-{}", this.shardId, this.selfId,
//							this.shardId, j, lastSeqs[j], lastSeq);
					lastSeqs[j] = lastSeq;
					rocksJavaUtil.put(this.shardId + "_" + j, lastSeqs[j].toString());
				}
			} else {
				lastSeqs[j] = BigInteger.valueOf(-1);
			}
		}
		// logger.warn("node-({}, {}): lastSeqs-{}", this.shardId, this.selfId,
		// JSONObject.toJSONString(lastSeqs));

	}

	private void getNewEventBodys() {
		// logger.warn("node-({}, {}): getNewEventBodys()...", this.shardId,
		// this.selfId);
		page++;
		eventBodys = getAllEvent4DB(page);
		num = eventBodys.size();
		index = 0;
//        flag = (num==sizePerCycle);
	}

	private List<EventBody> getAllEvent4DB(int page) {
		RocksJavaUtil rocksJavaUtil = new RocksJavaUtil(this.dbId);
		ArrayList<EventBody> list = new ArrayList<>();
		int n = 0;
		for (int creatorId = 0; creatorId < this.n; creatorId++) {
			BigInteger startSeq = BigInteger.valueOf(page).multiply(BigInteger.valueOf(this.sizePerHashnetNode));
			BigInteger endSeq = BigInteger.valueOf(page + 1).multiply(BigInteger.valueOf(this.sizePerHashnetNode));
			if (endSeq.compareTo(lastSeqs[creatorId]) >= 0) {
				n++;
				endSeq = lastSeqs[creatorId].add(BigInteger.ONE);
			} else {
				flag = true;
			}
			for (BigInteger seq = startSeq; seq.compareTo(endSeq) < 0; seq = seq.add(BigInteger.ONE)) {
				EventKeyPair pair = new EventKeyPair(this.shardId, creatorId, seq.longValue());
				byte[] evt = rocksJavaUtil.get(pair.toString());
				if (null != evt && evt.length > 0) {
					list.add(JSONObject.parseObject(new String(evt), EventBody.class));
//                    } else {
//                        logger.warn("node-({}, {}): getAllEvent4DB() : event-({},{},{}) id missing in database!!!",
//                                this.shardId, this.selfId, this.shardId, creatorId, seq);
				}
			}

		}
		if (n == this.n) {
			flag = false;
		}
//		logger.warn("node-({}, {}): getAllEvent4DB() : lastseqs: {}, n = {}, page = {}, event size: {}", this.shardId,
//				this.selfId, JSONArray.toJSONString(lastSeqs), this.n, page, list.size());
		return list;
	}
}
