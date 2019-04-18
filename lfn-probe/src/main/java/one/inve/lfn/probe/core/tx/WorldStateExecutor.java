package one.inve.lfn.probe.core.tx;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.alibaba.fastjson.JSONArray;

import one.inve.bean.message.SnapshotMessage;
import one.inve.bean.message.TransactionMessage;
import one.inve.beans.dao.Message;
import one.inve.contract.MVM.WorldStateService;
import one.inve.core.Config;

/**
 * abandon all validation activities at a time
 * 
 * @param m
 * @param dbId
 */
public class WorldStateExecutor {

	private void setBalance(Map<String, BigInteger> balance, String fromAddr, String toAddr, BigInteger value) {
		if (balance.get(fromAddr) == null)
			balance.put(fromAddr, new BigInteger("0"));
		if (balance.get(toAddr) == null)
			balance.put(toAddr, new BigInteger("0"));

		balance.put(fromAddr, balance.get(fromAddr).subtract(value));
		balance.put(toAddr, balance.get(toAddr).add(value));
	}

	public Map<String, BigInteger> exeTransaction(Message m, String dbId, boolean reestablishWorldState) {
		Map<String, BigInteger> balance = new HashMap<>();
		String sMessage = m.getMessage();
		TransactionMessage tx = JSONArray.parseObject(sMessage, TransactionMessage.class);

		BigInteger amount = null;
		String fromAddress = null;
		String toAddress = null;
		BigInteger fee = null;

		if (m.isValid()) {
			amount = tx.getAmount();
			fromAddress = tx.getFromAddress();
			toAddress = tx.getToAddress();
			fee = tx.getFee().multiply(tx.getNrgPrice());

			if (fromAddress.equals("KJNAOF724OLEWMRR2VFVDZMYIKCM5VPU")
					|| toAddress.equals("KJNAOF724OLEWMRR2VFVDZMYIKCM5VPU")) {
				System.out.println(fromAddress + " " + toAddress + " " + amount);
			}

			setBalance(balance, fromAddress, toAddress, amount);
			if (reestablishWorldState) {
				WorldStateService.transfer(dbId, fromAddress, toAddress, amount);
			}

			if (fee.compareTo(BigInteger.ZERO) > 0) {
				setBalance(balance, fromAddress, Config.FOUNDATION_ADDRESS, fee);
				if (reestablishWorldState) {
					boolean transferred = WorldStateService.transfer(dbId, fromAddress, Config.FOUNDATION_ADDRESS, fee);
					System.err.println("have a failure(" + m.getId() + ") from [" + fromAddress + "] to ["
							+ Config.FOUNDATION_ADDRESS + "] " + "[" + fee + "]");
				}

			}
		} else {
			System.err.println("unvalid message with id:" + m.getId());
		}

		return balance;
	}

	public Map<String, BigInteger> exeSnapshot(Message m, String dbId, boolean reestablishWorldState)
			throws InterruptedException {
		Map<String, BigInteger> balance = new HashMap<>();
		String sMessage = m.getMessage();
		SnapshotMessage snapshotMessage = JSONArray.parseObject(sMessage, SnapshotMessage.class);

		if (m.isValid()) {
			handleRewardOfSnapshot(snapshotMessage, m.geteHash(), dbId, balance, reestablishWorldState);
		}

		return balance;
	}

	/**
	 * 
	 * The snippet codes came from
	 * <code>ConsensusMessageHandleThread::handleRewardOfSnapshot</code> with
	 * deliberate abandonment of validation
	 */
	private void handleRewardOfSnapshot(SnapshotMessage snapMsg, String eHash, String dbId,
			Map<String, BigInteger> balance, boolean reestablishWorldState) throws InterruptedException {

		// 发放奖励
		ConcurrentHashMap<String, Long> contributions = snapMsg.getSnapshotPoint().getContributions();
		Collection<Long> cValues = contributions.values();
		// 计算总贡献数，当总贡献数量为0时，即无需任何奖励，则直接结束，否则根据每个地址占总贡献比例计算奖励
		BigDecimal totalContribution = BigDecimal.ZERO;
		for (Long v : cValues) {
			if (null != v) {
				totalContribution.add(BigDecimal.valueOf(v));
			}
		}
		if (totalContribution.equals(BigDecimal.ZERO)) {
			return;
		}

		double rewardRatio = snapMsg.getSnapshotPoint().getRewardRatio();
		BigInteger totalFee = snapMsg.getSnapshotPoint().getTotalFee();
		for (Map.Entry<String, Long> entry : contributions.entrySet()) {
			if (null != entry.getValue()) {
				BigInteger amount = new BigDecimal("" + rewardRatio).multiply(new BigDecimal(totalFee))
						.multiply(BigDecimal.valueOf(entry.getValue()))
						.divide(totalContribution, 0, BigDecimal.ROUND_HALF_UP).toBigInteger();
				if (amount.equals(BigInteger.ZERO)) {
					continue;
				}

				// 变更世界状态-发放节点奖励金
				setBalance(balance, Config.FOUNDATION_ADDRESS, entry.getKey(), amount);
				if (reestablishWorldState) {
					WorldStateService.transfer(dbId, Config.FOUNDATION_ADDRESS, entry.getKey(), amount);
				}

			}

		}
	}

}
