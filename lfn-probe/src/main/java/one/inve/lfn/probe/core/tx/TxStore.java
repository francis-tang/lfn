package one.inve.lfn.probe.core.tx;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONException;

import one.inve.beans.dao.Message;
import one.inve.beans.dao.TransactionSplit;
import one.inve.core.Config;
import one.inve.db.transaction.MysqlHelper;
import one.inve.rocksDB.RocksJavaUtil;

public class TxStore {
	public List<Message> getMessages(String dbId, BigInteger tableIndex, String address) {
		List<Message> messages = new ArrayList<>();
		MysqlHelper h = null;

		try {
			if (null != tableIndex && tableIndex.compareTo(BigInteger.ZERO) >= 0) {

				h = new MysqlHelper(dbId);
				messages = getMessages(messages, h, tableIndex, dbId, address);

				// 查询不到，看是否有下一个表，如果有，继续查询
				if (messages == null || messages.size() < 1) {
					tableIndex = tableIndex.add(BigInteger.ONE);
					// tableIndex跳到下一张表后,offset从零开始
					messages = getMessages(messages, h, tableIndex, dbId, address);
				}
			}
		} catch (Exception ex) {

		} finally {
			if (h != null) {
				h.destroyed();
			}
		}
		return messages;
	}

	/**
	 * 查询表是否存在
	 * 
	 * @return 交易表索引
	 */
	public static TransactionSplit tableExist(String dbId) {
		// 是否存在TransactionSplit
		TransactionSplit split = null;
		try {
			byte[] value = new RocksJavaUtil(dbId).get(Config.MESSAGES);
			if (value != null) {
				String json = new String(value);
				split = JSONArray.parseObject(json, TransactionSplit.class);
			}
		} catch (Exception e) {
		}
		return split;
	}

	private List<Message> getMessages(List<Message> messages, MysqlHelper h, BigInteger tableIndex, String dbId,
			String address) {
		// logger.info("tableExist...");
		TransactionSplit split = tableExist(dbId);
		// logger.info("getTableIndex...");
		BigInteger splitIndex = split.getTableIndex();

		if (tableIndex != null && tableIndex.compareTo(splitIndex) > 0) {
			return messages;
		}
		String tableName = Config.MESSAGES + Config.SPLIT + tableIndex;
		StringBuilder sql = new StringBuilder("select hash from ");
		sql.append(tableName);
		// List<Message> entityArrayList = array.getList();

		sql.append(" where fromAddress='" + address + "' OR toAddress='" + address + "'");
		sql.append(" order by id asc ");

		try {
			List<Message> freshMessages = h.executeQuery(sql.toString(), (rs, index) -> {
				String hash = rs.getString("hash");

				byte[] transationByte = new RocksJavaUtil(dbId).get(hash);
				if (transationByte != null) {

					try {
						Message m = JSONArray.parseObject(transationByte, Message.class);
						return m;
					} catch (JSONException e) {
						System.err.println("WWIBETBCSBET - transationByte=" + new String(transationByte));
						return null;
					}
				} else {
					// logger.error("this hash rocksDB not exist");
					return null;
				}
			});

			if (freshMessages != null) {
				messages.addAll(freshMessages);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return messages;
	}
}
