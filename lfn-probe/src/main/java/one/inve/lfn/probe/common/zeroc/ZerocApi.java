package one.inve.lfn.probe.common.zeroc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * 
 * Copyright © CHXX Co.,Ltd. All rights reserved.
 * 
 * @Description: encapsulate the access to zeroc api and business impl based on
 *               zeroc.
 * @see one/inve/lfn/probe/common/zeroc/feedback.ice
 * @author: Francis.Deng
 * @date: 2019年3月26日 上午10:52:52
 * @version: V1.0
 */
public class ZerocApi {
	private static final Logger logger = LoggerFactory.getLogger(ZerocApi.class);

	public void startFeedbackServer(String[] cmdArgs, String iceObjectName, String port) {
		try (com.zeroc.Ice.Communicator communicator = com.zeroc.Ice.Util.initialize(cmdArgs)) {
			com.zeroc.Ice.ObjectAdapter adapter = communicator
					.createObjectAdapterWithEndpoints(iceObjectName + "Adapter", "default -p " + port);
			com.zeroc.Ice.Object object = new FeedbackImpl();
			adapter.add(object, com.zeroc.Ice.Util.stringToIdentity(iceObjectName));
			adapter.activate();
			communicator.waitForShutdown();
		}
	}

	public String callFeedbackNow(String[] cmdArgs, String iceObjectName, String ipAddress, String port) {
		try (com.zeroc.Ice.Communicator communicator = com.zeroc.Ice.Util.initialize(cmdArgs)) {
			com.zeroc.Ice.ObjectPrx base = communicator
					.stringToProxy(String.format("%s:default -h %s -p %s", iceObjectName, ipAddress, port));// SimplePrinter:default
																											// -h 111 -p
																											// 10000

			FeedbackPrx printer = FeedbackPrx.checkedCast(base);

			return printer.now();
		}
	}
}
