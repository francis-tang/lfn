package one.inve.lfn.probe.core.tx;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import one.inve.bean.message.TransactionMessage;
import one.inve.beans.dao.Message;
import one.inve.beans.dao.TransactionArray;
import one.inve.db.transaction.MysqlHelper;
import one.inve.db.transaction.QueryTableSplit;

/**
 * 
 * 
 * Copyright © CHXX Co.,Ltd. All rights reserved.
 * 
 * @Description: care about WorldState things
 * @author: Francis.Deng
 * @date: 2019年4月8日 下午8:05:17
 * @version: V1.0
 */
public class WorldStateParadigm {

	// attempt to rebuild world state by feeding normal message and snapshot message
	// and collect my report.
	public Map<String, BigInteger> reestablish(String dbId, boolean reestablishWorldState) {
		Map<String, BigInteger> balance = new HashMap<>();

		BigInteger zeroTableIndex = new BigInteger("0");
		long offset = 0L;
		WorldStateExecutor wse = new WorldStateExecutor();
		Integer type = 1;
		int messageItemNum = 0;

		// notorious glitch to force mysql to using RSA encryption.
		MysqlHelper mh = new MysqlHelper(dbId, false);
		int sizeOfTransaction = 0;

		List<TransactionMessage> txMessages = new ArrayList<>();

		// process normal message(1-trans , 2-contract, 3-shapshot, 4-text)
		do {
			TransactionArray ta = queryTransaction(zeroTableIndex, offset, type, dbId);

			for (Message m : ta.getList()) {
				if (m != null) {
					// balance.putAll(wse.exeTransaction(m, dbId, reestablishWorldState));
					balance = mergeBalance(balance, wse.exeTransaction(m, dbId, reestablishWorldState));

//					TransactionMessage tx = JSONArray.parseObject(m.getMessage(), TransactionMessage.class);
//					if (tx.getFromAddress().equals("KJNAOF724OLEWMRR2VFVDZMYIKCM5VPU")
//							|| tx.getToAddress().equals("KJNAOF724OLEWMRR2VFVDZMYIKCM5VPU")) {
//						txMessages.add(tx);
//					}
				}
			}

			messageItemNum = ta.getList().size();
			zeroTableIndex = ta.getTableIndex();
			offset = ta.getOffset();

			sizeOfTransaction += messageItemNum;

		} while (messageItemNum != 0);

		// process snapshot message(1-trans , 2-contract, 3-shapshot, 4-text)
		zeroTableIndex = new BigInteger("0");
		offset = 0L;
		type = 3;
		do {
			TransactionArray ta = queryTransaction(zeroTableIndex, offset, type, dbId);

			for (Message m : ta.getList()) {
				if (m != null) {
					try {
						// balance.putAll(wse.exeSnapshot(m, dbId, reestablishWorldState));
						balance = mergeBalance(balance, wse.exeSnapshot(m, dbId, reestablishWorldState));
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}

			messageItemNum = ta.getList().size();
			zeroTableIndex = ta.getTableIndex();
			offset = ta.getOffset();

		} while (messageItemNum != 0);

		return balance;

	}

	private Map<String, BigInteger> mergeBalance(Map<String, BigInteger> large, Map<String, BigInteger> small) {
		Map<String, BigInteger> merged = large;

		small.forEach((k, v) -> {
			if (merged.get(k) == null)
				merged.put(k, new BigInteger("0"));

			merged.put(k, merged.get(k).add(v));
		});

		return merged;
	}

	// attempt to rebuild world state by feeding normal message and snapshot message
//	public void reestablish(String dbId) {
//		BigInteger zeroTableIndex = new BigInteger("0");
//		WorldStateExecutor wse = new WorldStateExecutor();
//		long offset = 50L;// fetch data set every 50
//		Integer type = 1;
//		int messageItemNum = 0;
//
//		// notorious glitch to force mysql to using RSA encryption.
//		MysqlHelper mh = new MysqlHelper(dbId, false);
//
//		// process normal message(1-trans , 2-contract, 3-shapshot, 4-text)
//		do {
//			TransactionArray ta = queryTransaction(zeroTableIndex, offset, type, dbId);
//
//			ta.getList().forEach((m) -> {
//
//				// later on executing this tx(inside message) badly
//				wse.exeTransaction(m, dbId, true);
//
//			});
//
//			messageItemNum = ta.getList().size();
//			zeroTableIndex = ta.getTableIndex();
//			offset = ta.getOffset();
//
//		} while (messageItemNum != 0);
//
//		// process snapshot message(1-trans , 2-contract, 3-shapshot, 4-text)
//		type = 3;
//		do {
//			TransactionArray ta = queryTransaction(zeroTableIndex, offset, type, dbId);
//
//			ta.getList().forEach((m) -> {
//
//				// later on executing this tx(inside message) badly
//				try {
//					wse.exeSnapshot(m, dbId, true);
//				} catch (InterruptedException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//			});
//
//			messageItemNum = ta.getList().size();
//			zeroTableIndex = ta.getTableIndex();
//			offset = ta.getOffset();
//
//		} while (messageItemNum != 0);
//
//	}

	protected synchronized TransactionArray queryTransaction(BigInteger tableIndex, long offset, Integer type,
			String dbId) {
		QueryTableSplit queryTableSplit = new QueryTableSplit();
		return queryTableSplit.queryTransaction(tableIndex, offset, type, dbId);
	}

}
