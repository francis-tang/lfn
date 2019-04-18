package one.inve.lfn.probe.ping;

import one.inve.lfn.probe.common.zeroc.ZerocApi;

/**
 * 
 * 
 * Copyright © CHXX Co.,Ltd. All rights reserved.
 * 
 * @Description: start feedback server which allows client side to interact with
 *               feedback via zeroc api.
 * @see one.inve.lfn.probe.common.zeroc.ZerocApi
 * @author: Francis.Deng
 * @date: 2019年3月26日 上午10:39:55
 * @version: V1.0
 */
public class Server {

	public static void main(String[] args) {
		if (args.length >= 2) {
			String iceObjectName = args[0];
			String port = args[1];

			ZerocApi zerocApi = new ZerocApi();
			zerocApi.startFeedbackServer(args, iceObjectName, port);
		}

	}
}
