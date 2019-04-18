
package one.inve.lfn.probe.core.metrics;

import java.math.BigInteger;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;

import one.inve.beans.dao.TransactionSplit;
import one.inve.core.Config;
import one.inve.db.transaction.MysqlHelper;
import one.inve.lfn.probe.common.db.RocksJavaUtil;

/**
 * 
 * Copyright © CHXX Co.,Ltd. All rights reserved.
 * 
 * @Description: INVEMetricsManager maintains a INVEMetrics,which originates
 *               from rocksdb or accumulates the values at runtime.
 * @author: Francis.Deng
 * @date: 2019年3月22日 上午10:29:34
 * @version: V1.0
 */
public class INVEMetricsManager {
	private static final Logger logger = LoggerFactory.getLogger("metrics");

	private INVEMetrics metrics = new INVEMetrics();

	public INVEMetricsManager(String dbId, String rocksdbPath) {
		init(dbId, rocksdbPath);
	}

	// <code>DbUtils::initStatistics</code>
	private void init(String dbId, String rocksdbPath) {
		// RocksJavaUtil rocksJavaUtil = new RocksJavaUtil(dbId);
		RocksJavaUtil rocksJavaUtil = new RocksJavaUtil(dbId, rocksdbPath);

		// 共识Event总数
		byte[] totalConsEventCount = rocksJavaUtil.get(Config.CONS_EVT_COUNT_KEY);
		metrics.consensualEventCount = (null == totalConsEventCount ? BigInteger.ZERO
				: new BigInteger(new String(totalConsEventCount)));

		// 共识message总数和最大Id
		BigInteger idx = BigInteger.ZERO;
		byte[] transaSplitBytes = rocksJavaUtil.get(Config.MESSAGES);
		if (null != transaSplitBytes) {
			TransactionSplit tableInfo = JSONObject.parseObject(new String(transaSplitBytes), TransactionSplit.class);
			idx = tableInfo.getTableIndex();
		}
		MysqlHelper h = null;
		List<BigInteger> list = null;
		try {
			// h = new MysqlHelper(dbId);// question to be raised??
			h = new MysqlHelper(dbId, false);

			String sql = "select max(id) as id from " + Config.MESSAGES + Config.SPLIT + idx.toString();
			list = h.executeQuery(sql, (rs, index) -> new BigInteger(rs.getString("id")));
		} catch (Exception ex) {
			logger.error("error: {}", ex);
			System.exit(-1);
		} finally {
			if (h != null) {
				h.destroyed();
			}
		}
		if (null != list && list.size() > 0) {
			metrics.consensualMessageMaxId = (list.get(0));
			metrics.consensualMessageCount = metrics.consensualMessageMaxId;// question to be raised - why is the two
																			// equal?

			byte[] consMessageMaxId = rocksJavaUtil.get(Config.CONS_MSG_COUNT_KEY);
			if (null == consMessageMaxId || !new BigInteger(new String(consMessageMaxId)).equals(list.get(0))) {
//				logger.warn("node-({},{}): fix Config.CONS_MSG_COUNT_KEY value from {} to {}", node.getShardId(),
//						node.getCreatorId(), new String(consMessageMaxId), list.get(0));
				logger.debug("fix Config.CONS_MSG_COUNT_KEY value from {} to {}", new String(consMessageMaxId),
						list.get(0));
				rocksJavaUtil.put(Config.CONS_MSG_COUNT_KEY, list.get(0).toString());// fix Config.CONS_MSG_COUNT_KEY
																						// value from 974 to 978
			}
		} else {
			logger.error("Inve did not creation.");
			System.exit(-1);
		}

		// Event总数
		byte[] eventCount = rocksJavaUtil.get(Config.EVT_COUNT_KEY);
		metrics.totalEventCount = (null == eventCount ? BigInteger.ZERO : new BigInteger(new String(eventCount)));

		// 系统自动生成交易总数
		byte[] sysTxCount = rocksJavaUtil.get(Config.SYS_TX_COUNT_KEY);
		metrics.systemAutoTxMaxId = (null == sysTxCount ? BigInteger.ZERO : new BigInteger(new String(sysTxCount)));
	}

	public INVEMetrics getMetrics() {
		return metrics;
	}
}
