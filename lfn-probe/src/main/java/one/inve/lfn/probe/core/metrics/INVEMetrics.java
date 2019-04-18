
/**
 * 
 * Copyright © CHXX Co.,Ltd. All rights reserved.
 * 
 * @Description: turn localfullnode function into more modular,more testable
 *               function body.
 * @author: Francis.Deng
 * @date: 2019年3月22日 上午10:29:34
 * @version: V1.0
 */
package one.inve.lfn.probe.core.metrics;

import java.math.BigInteger;

public class INVEMetrics {
	BigInteger consensualEventCount;// event count after reaching a consensus(setTotalConsEventCount)
	BigInteger consensualMessageCount;// message count after reaching a consensus(setConsMessageCount)

	BigInteger consensualMessageMaxId;// (setConsMessageMaxId)

	BigInteger totalEventCount;// setTotalEventCount
	BigInteger systemAutoTxMaxId;// setSystemAutoTxMaxId

	public BigInteger getConsensualEventCount() {
		return consensualEventCount;
	}

	public BigInteger getConsensualMessageCount() {
		return consensualMessageCount;
	}

	public BigInteger getConsensualMessageMaxId() {
		return consensualMessageMaxId;
	}

	public BigInteger getTotalEventCount() {
		return totalEventCount;
	}

	public BigInteger getSystemAutoTxMaxId() {
		return systemAutoTxMaxId;
	}

	@Override
	public String toString() {
		return "INVEMetrics [consensualEventCount=" + consensualEventCount + ", consensualMessageCount="
				+ consensualMessageCount + ", consensualMessageMaxId=" + consensualMessageMaxId + ", totalEventCount="
				+ totalEventCount + ", systemAutoTxMaxId=" + systemAutoTxMaxId + "]";
	}

}
