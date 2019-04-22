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
 * @Description: Quickly fix lastSeq loss issue.There is a good performance if
 *               you have initialLastSeqs in place.
 * @author: Francis.Deng
 * @date: Apr 22, 2019 2:16:25 AM
 * @version: V1.0
 */
public class BottomUPLastSeqs {

	public static void main(String[] args) {
		StringHolder dbId = new StringHolder();
		IntHolder shardId = new IntHolder();
		IntHolder howManyMembersInOneShard = new IntHolder();
		StringHolder initialLastSeqs = new StringHolder();

		CliParser parser = new CliParser(
				"java -cp ./lfn-probe-0.5.0.jar:./localfullnode-2.0.0.jar one.inve.lfn.probe.recovery.BottomUPLastSeqs");
		parser.addOption("-db,--dbid %s #database id", dbId);
		parser.addOption("-sid,--shardId %i #shard id", shardId);
		parser.addOption("-m,--howManyMembersInOneShard %i #how many members in one shard", howManyMembersInOneShard);
		parser.addOption("-ils,--initialLastSeqs %s #initial last seqs", initialLastSeqs);

		parser.matchAllArgs(args);

		BigInteger[] lastSeqs = getlastSeqs(howManyMembersInOneShard.value, dbId.value, shardId.value,
				stringToBigIntegers(initialLastSeqs.value), true);

		System.out.println(Arrays.toString(lastSeqs));

	}

	private static BigInteger[] stringToBigIntegers(String string) {
		String[] ss = string.split(",");
		BigInteger[] initialLastSeqs = new BigInteger[ss.length];

		for (int i = 0; i < initialLastSeqs.length; i++) {
			initialLastSeqs[i] = new BigInteger(ss[i]);
		}

		return initialLastSeqs;
	}

	private static BigInteger[] getlastSeqs(int n, String dbId, int shardId, BigInteger[] initialLastSeqs,
			boolean retained) {
		BigInteger[] lastSeqs = new BigInteger[n];
		RocksJavaUtil rocksJavaUtil = new RocksJavaUtil(dbId);

		for (int j = 0; j < n; j++) {
			BigInteger lastSeq = initialLastSeqs == null ? BigInteger.ZERO : initialLastSeqs[j];
			BigInteger stableLastSeq = BigInteger.ZERO;
			int fetchNothingInARow = 0;

			while (fetchNothingInARow <= 5000000) {
//				System.out.println("lastSeq=" + lastSeq);
//				System.out.println("stableLastSeq=" + stableLastSeq);

				EventKeyPair pair = new EventKeyPair(shardId, j, lastSeq.longValue());
				byte[] ebByte = rocksJavaUtil.get(pair.toString());

				if (null == ebByte || ebByte.length <= 0) {
					fetchNothingInARow++;
				} else {
					stableLastSeq = lastSeq;
					fetchNothingInARow = 0;
				}
				lastSeq = lastSeq.add(BigInteger.ONE);
			}

			lastSeqs[j] = stableLastSeq;

			if (retained) {
				rocksJavaUtil.put(shardId + "_" + j, lastSeqs[j].toString());
			}
		}

		return lastSeqs;

	}
}
