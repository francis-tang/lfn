package one.inve.lfn.probe.ping;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import one.inve.lfn.probe.common.zeroc.ZerocApi;

/**
 * 

 * Copyright © CHXX Co.,Ltd. All rights reserved.  
 *   
 * @Description: 
@formatter:off

Linux commands as followed:
run ping server:
java -cp lfn-probe-0.5.0.jar:localfullnode-2.0.0.jar one.inve.lfn.probe.ping.Server feedback 50077
run continuous ping()
java -cp lfn-probe-0.5.0.jar:localfullnode-2.0.0.jar one.inve.lfn.probe.ping.Command feedback 3.94.202.66:50077;54.213.84.89:50077;3.121.162.184:50077;13.250.14.98:50077;13.211.5.129:50077

Windows commands as followed:
run ping server:
java -cp lfn-probe-0.5.0.jar;localfullnode-2.0.0.jar one.inve.lfn.probe.ping.Server feedback 50077
run continuous ping()
java -cp lfn-probe-0.5.0.jar;localfullnode-2.0.0.jar one.inve.lfn.probe.ping.Command feedback 127.0.0.1:50077


@formatter:on
 * @author: Francis.Deng 
 * @date: 2019年3月26日 上午10:08:10 
 * @version: V1.0
 */
public class Command {
	private static final Logger logger = LoggerFactory.getLogger("ping");

	public static void main(String[] args) {
		String iceObjectName = null;
		String memberNames = null;

		if (args.length >= 2) {
			iceObjectName = args[0];
			memberNames = args[1];

		} else if (args.length == 1) {
			iceObjectName = args[0];
			memberNames = "3.94.202.66:50077;54.213.84.89:50077;3.121.162.184:50077;13.250.14.98:50077;13.211.5.129:50077";
		}

		ZerocApi zerocApi = new ZerocApi();

		List<Socket> sockets = parseSocket(memberNames);

		while (true) {
			try {
				for (Socket s : sockets) {
					Instant time1 = Instant.now();
					zerocApi.callFeedbackNow(args, iceObjectName, s.ipAddress, s.port);
					long interval = Duration.between(time1, Instant.now()).toMillis();
					logger.info("{} milliseconds - roundtrip to {}:{} costs ", String.valueOf(interval), s.ipAddress,
							s.port);
				}
			} catch (Exception e) {
				logger.error(e.getClass().getName());
			}

			try {
				Thread.currentThread().sleep(5000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

	}

	private static List<Socket> parseSocket(String socketStr) {
		List<Socket> ret = new ArrayList<Socket>();

		try {
			if (socketStr != null && !socketStr.equals("")) {
				String parts[] = socketStr.split(";");

				for (String part : parts) {
					String[] pieces = part.split(":");

					Socket s = new Socket(pieces[0], pieces[1]);
					ret.add(s);
				}

			}
		} catch (Exception e) {
			logger.error(e.getMessage());
		}

		return ret;
	}

	private static class Socket {
		public String ipAddress;
		public String port;

		public Socket(String ipAddress, String port) {
			super();
			this.ipAddress = ipAddress;
			this.port = port;
		}

	}

}
