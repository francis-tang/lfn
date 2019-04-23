package one.inve.lfn.probe.recovery;

import java.math.BigInteger;
import java.security.PublicKey;
import java.util.Arrays;

import com.alibaba.fastjson.JSONObject;

import one.inve.core.Cryptos;
import one.inve.core.EventBody;
import one.inve.core.EventKeyPair;
import one.inve.core.Hash;
import one.inve.lfn.probe.common.cli.CliParser;
import one.inve.lfn.probe.common.cli.IntHolder;
import one.inve.lfn.probe.common.cli.StringHolder;
import one.inve.rocksDB.RocksJavaUtil;
import one.inve.util.HnKeyUtils;

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
				stringToBigIntegers(initialLastSeqs.value), false);

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
		PublicKey[][] publicKeys = getPublicKeys();

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
					// introduce verification mechanism from EventFlow::addEvent
					EventBody me = JSONObject.parseObject(new String(ebByte), EventBody.class);
					EventBody selfParent = getEvent(dbId, me.getShardId(), me.getCreatorId(), me.getCreatorSeq() - 1);
					EventBody otherParent = getEvent(dbId, me.getShardId(), me.getOtherId(), me.getOtherSeq());

					if (selfParent == null && me.getCreatorSeq() - 1L != -1L) {
						System.err.println("a failure because selfParent (id,seq) of (" + me.getCreatorId() + ", "
								+ (me.getCreatorSeq()) + ") is missing");
						lastSeq = lastSeq.add(BigInteger.ONE);
						continue;
					}
					if (otherParent == null && me.getOtherId() != -1L && me.getOtherSeq() != -1L) {
						System.err.println("a failure because otherParent (id, seq) of (" + me.getOtherId() + ", "
								+ me.getOtherSeq() + ") is missing ");
						lastSeq = lastSeq.add(BigInteger.ONE);
						continue;
					}
					// 如果创建时间比自己的父节点还早，则仍然不合法
					if (selfParent != null && !me.getTimeCreated().isAfter(selfParent.getTimeCreated())) {
						System.err.println("a failure because (id, seq) of (" + me.getCreatorId() + ", "
								+ me.getCreatorSeq() + ")  timecreated is before than its parent ");
						lastSeq = lastSeq.add(BigInteger.ONE);
						continue;
					}
					// 本地数据
					byte[] selfParentHash = selfParent == null ? null : selfParent.getHash();
					// 本地数据
					byte[] otherParentHash = otherParent == null ? null : otherParent.getHash();
					// 本地数据
					byte[] selfHash = Hash.hash(me.getShardId(), me.getCreatorId(), me.getCreatorSeq(), selfParentHash,
							otherParentHash, me.getTimeCreated(), me.getTrans());

					PublicKey publicKey = publicKeys[me.getShardId()][(int) me.getCreatorId()];
					if (!Cryptos.verifySignature(selfHash, me.getSignature(), publicKey)) {
						System.err.println(me.getCreatorSeq() + " Cryptos.verifySignature(<" + selfHash + ">,<"
								+ me.getSignature() + ">) failure ");
						lastSeq = lastSeq.add(BigInteger.ONE);
						continue;
					} else {
						stableLastSeq = lastSeq;
						fetchNothingInARow = 0;
					}

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

	// retrieve from product environment(10)
	private static PublicKey[][] getPublicKeys() {
		PublicKey[][] publicKeys = null;

		try {
			publicKeys = new PublicKey[1][10];

			publicKeys[0][0] = HnKeyUtils.getPublicKey4String(
					"MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQC5bARpAKqMejcxwMXJpNW0abl7TiI2Mgxx3ZPUNxqBZjKcOTCtRwWpwTRrMNG7NrVnQOVaDYx8GVE6a9yexolHQPP6tWZQa7wyvB8qpLHZdnKAyL4zYTHmG1L1E6vr+deeTXfCXspsucgTh81KIpLAO52kmRnsD/74F0/lmMCw2wIDAQAB");
			publicKeys[0][1] = HnKeyUtils.getPublicKey4String(
					"MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCE8svr39JWPKaCDGtaidHXG0saiNfUEF1mgnJRNjxtoj25pf4bcsiXF4qa32HYW1vvKHS3BXSNV8qkrIRqZloCPFCiyHIT3T6uHkXJYAP+/Vctn3dlNtcg5MsuentH4WM6uxyhy5ym0n7lh9EIbdVo+9/1baPTF3bdSlioXu0aDwIDAQAB");
			publicKeys[0][2] = HnKeyUtils.getPublicKey4String(
					"MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCDHOm8oTVbtMQbcMiF9d1YjRnJ+120uQ1hmWt00qXxyFJ1HkECw5eeTyWuvNztOSrj4Kc1HtzXoRPPuY0H4X5oB/WSj16UeWeLXkNkm8dopCHYM5MDNlhBWdkYh3MofJ1VgkZJ8JW8x96kiaJIk6UOkLESWAuIYYqcwndV3NPFQwIDAQAB");
			publicKeys[0][3] = HnKeyUtils.getPublicKey4String(
					"MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCqPk/Rf+/18mOX1qzsc4/3AZRE2Ggl+sg6JkfXrXgkZRGcm/0b1e8OOyR5zMiPj0aVheB5LPR6QeXxWs5HUnceEbgdTgdJq4jzz55RBvIgKISq0MtcQKj7wQO/oy6RqjS1KlkfJ2vArXuaNAAeariCd2Mew6tC3QhmJwCaKUZQDQIDAQAB");
			publicKeys[0][4] = HnKeyUtils.getPublicKey4String(
					"MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQDfstb7HwOv4PoebdEKV+QwqkG5TqX47EC8+j7SnlyrAKMQ0fkkIqix3DavoZt3CD0skGs4z3PEM4bt5O0xU0WYqsnrTvOvl2hN91P3vqEeN/txE+sPaO3rRGWbrcxwf8Efdj8hdXSQcd+jxlsdfRfqKia8StPNJiAj4Ad87A2uzQIDAQAB");
			publicKeys[0][5] = HnKeyUtils.getPublicKey4String(
					"MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCOImyQLjDl70rwsI6M2GIelVm4yaC9DZcLAcnpx0f8/pAkcUNYOgIYajiIrJy93sKzFJTTcrVB4TEI9RRsuCBTDebn53vGqjLVk+7Iw97IPh0NpyRc7ZCsXKtUVL2CVCmqN9/noZaOAUm1Ckn2kAaeD+YVx2E1rIDtygBkNQB5kQIDAQAB");
			publicKeys[0][6] = HnKeyUtils.getPublicKey4String(
					"MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQD4ze04jpMEPYhZMRjbnMPswEE9fWYKoLMIG35OC+pvv9svM0VZk0I4L5578klNIK62jd1EkV9lWTQz5S83J4oqB9jpt0xfJjB0VezLA7Ht9oJi1x0hJNCt6agubJ6JtrA+omLIkMEW082xAjkwDFrPUniCX2PHD3chGPDOqO69ZQIDAQAB");
			publicKeys[0][7] = HnKeyUtils.getPublicKey4String(
					"MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCDYx1QbWifvt0P5qEJyvQbVgwl2g7GQyaWXSenKxA0GhE4Ca/NIsnY8krqgDwndnCQ8ILJFx0QflH91ZG3Ajeib9FSXfgSa4NPyAUPV2ChC5FfK+XDAGZEspqkFMQQTupt69bsAxWckHhsWrdJcO6iXjFMPInfbKVbNol7OH8tZQIDAQAB");
			publicKeys[0][8] = HnKeyUtils.getPublicKey4String(
					"MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCGmr0Pv/nQiPfrqK2juQ7O4/s0zkzc+MRP63wmgZdccbzwAQqrBgTYlIK5uJQQqUdnYOBqX7J8KIzKFPYczCykhklafQ4RPdbup9iTtWofmS9fJR8zhJ/KH0WNZAQr3JPDkUCgG8S/HlM7gHELKg12KNxnBZhGeh01beAyhpzHpwIDAQAB");
			publicKeys[0][9] = HnKeyUtils.getPublicKey4String(
					"MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQDRIcGafO3nJg8NOINvySmTzQYRAn8YLjJ7NcMfs2d9CaKNid7UXzyIn4G3oeitUTMb3yl3aTotc1JlT3Zw7HfPLYu4oK7U++aSSyV8gD3Flp4y49Rzt74lbxxebm1Ip7p9jelCdDVzWA9KRnG1UAMvhyTmddRbAvLYurq77wQZBwIDAQAB");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return publicKeys;
	}

	private static EventBody getEvent(String dbId, int shardId, long creatorId, long creatorSeq) {
		EventBody event = null;
		RocksJavaUtil rocksJavaUtil = new RocksJavaUtil(dbId);
		EventKeyPair pair = new EventKeyPair(shardId, creatorId, creatorSeq);
		byte[] evt = rocksJavaUtil.get(pair.toString());
		if (null != evt && evt.length > 0) {
			event = JSONObject.parseObject(new String(evt), EventBody.class);
		}
		return event;
	}
}
