package one.inve.lfn.probe.core.metrics;

import org.junit.Test;

public class INVEMetricsManagerTest {
	@Test
	public void testInitFromRocksdb() {
		INVEMetricsManager inveMetricsManager = new INVEMetricsManager("0_5", "E:/landing/virginia_0_5/");
		INVEMetrics metrics = inveMetricsManager.getMetrics();

		System.out.println(metrics);

		// singapore:INVEMetrics [consensualEventCount=117881359,
		// consensualMessageCount=978, consensualMessageMaxId=978,
		// totalEventCount=117885258, systemAutoTxMaxId=625]
		//
		// virginia: [consensualEventCount=117881359, consensualMessageCount=978,
		// consensualMessageMaxId=978, totalEventCount=117885258, systemAutoTxMaxId=625]

	}
}
