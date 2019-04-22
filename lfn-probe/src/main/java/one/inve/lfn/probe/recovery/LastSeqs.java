package one.inve.lfn.probe.recovery;

import java.math.BigInteger;
import java.util.Arrays;

import one.inve.core.EventKeyPair;
import one.inve.lfn.probe.common.cli.CliParser;
import one.inve.lfn.probe.common.cli.IntHolder;
import one.inve.lfn.probe.common.cli.StringHolder;
import one.inve.rocksDB.RocksJavaUtil;

/**
 * 
 * 
 * Copyright © CHXX Co.,Ltd. All rights reserved.
 * 
 * @Description: rebuild last seq by retained last seq in rocksdb
 * @author: Francis.Deng
 * @date: Apr 21, 2019 8:31:19 PM
 * @version: V1.0
 */
public class LastSeqs {

	public static void main(String[] args) {
		StringHolder dbId = new StringHolder();
		IntHolder shardId = new IntHolder();
		IntHolder howManyMembersInOneShard = new IntHolder();

		CliParser parser = new CliParser(
				"java -cp ./lfn-probe-0.5.0.jar:./localfullnode-2.0.0.jar one.inve.lfn.probe.recovery.LastSeqs");
		parser.addOption("-db,--dbid %s #database id", dbId);
		parser.addOption("-sid,--shardId %i #shard id", shardId);
		parser.addOption("-m,--howManyMembersInOneShard %i #how many members in one shard", howManyMembersInOneShard);

		parser.matchAllArgs(args);

		BigInteger[] lastSeqs = getlastSeqs(howManyMembersInOneShard.value, dbId.value, shardId.value, false);

		System.out.println(Arrays.toString(lastSeqs));

	}

	/**
	 * mockup of EventIterator::getlastSeqs which plays a important to rebuild last
	 * seq during the recovery
	 * 
	 * @param n
	 * @param dbId
	 * @param shardId
	 * @param retained whether persist last sequences array or not
	 * @return
	 */
	private static BigInteger[] getlastSeqs(int n, String dbId, int shardId, boolean retained) {
		BigInteger[] lastSeqs = new BigInteger[n];
		RocksJavaUtil rocksJavaUtil = new RocksJavaUtil(dbId);
		for (int j = 0; j < n; j++) {
			byte[] seqByte = rocksJavaUtil.get(shardId + "_" + j);
			if (null != seqByte && seqByte.length > 0) {
				lastSeqs[j] = new BigInteger(new String(seqByte));

				BigInteger lastSeq = lastSeqs[j];
				EventKeyPair pair = new EventKeyPair(shardId, j, lastSeq.longValue());
				byte[] ebByte = rocksJavaUtil.get(pair.toString());
				if (null == ebByte || ebByte.length <= 0) {
					// 向前获取存在的Event
					while (null == ebByte || ebByte.length <= 0) {
						lastSeq = lastSeq.subtract(BigInteger.ONE);
						// if (lastSeq.equals(BigInteger.ZERO)) {
						if (lastSeq.compareTo(BigInteger.ZERO) == -1) {
							break;
						}
						pair = new EventKeyPair(shardId, j, lastSeq.longValue());
						ebByte = rocksJavaUtil.get(pair.toString());
					}
				} else {
					// 向后获取更新的Event
					while (null != ebByte && ebByte.length > 0) {
						lastSeq = lastSeq.add(BigInteger.ONE);

						pair = new EventKeyPair(shardId, j, lastSeq.longValue());
						ebByte = rocksJavaUtil.get(pair.toString());
					}
					lastSeq = lastSeq.subtract(BigInteger.ONE);
				}
				if (!lastSeqs[j].equals(lastSeq)) {
//					logger.warn("node-({}, {}): ({}, {}) lastSeq diff: db-{}, calcu-{}", this.shardId, this.selfId,
//							this.shardId, j, lastSeqs[j], lastSeq);
					lastSeqs[j] = lastSeq;

					if (retained) {
						rocksJavaUtil.put(shardId + "_" + j, lastSeqs[j].toString());
					}

				}
			} else {
				lastSeqs[j] = BigInteger.valueOf(-1);
			}
		}
		// logger.warn("node-({}, {}): lastSeqs-{}", this.shardId, this.selfId,
		// JSONObject.toJSONString(lastSeqs));

		return lastSeqs;

	}

}
