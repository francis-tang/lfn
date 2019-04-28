package one.inve.lfn.probe.ws;

import java.math.BigInteger;
import java.util.Map;

import one.inve.contract.MVM.WorldStateService;
import one.inve.contract.provider.RepositoryProvider;
import one.inve.lfn.probe.core.tx.WorldStateParadigm;

/**
 * 
 * 
 * Copyright © CHXX Co.,Ltd. All rights reserved.
 * 
 * @Description: re-establish world state after it's broken.
 * @author: Francis.Deng
 * @date: Apr 10, 2019 1:44:10 AM
 * @version: V1.0
 */
public class WorldStateReestablishment {

	public static void main(String[] args) {
		final String dbId;

		if (args != null && args.length >= 1) {
			dbId = args[0];

			RepositoryProvider.getTrack(dbId);

			WorldStateParadigm wsp = new WorldStateParadigm();
			// making sure one.inve.contract.MVM::WorldStateService::setBalance function in
			// place:
			// @formatter:off
			/**
				public static void setBalance(String dbId, String address, BigInteger value) {
					Repository track = getTrack(dbId);
			
					// 讀取餘額
					BigInteger balance = track.getBalance(address.getBytes());
					// 餘額清零
					track.addBalance(address.getBytes(), balance.negate());
					// 直接設置餘額
					track.addBalance(address.getBytes(), value);
					// force it to commit root
					((INVERepositoryRoot) track).commit(dbId);
				}
			 */
			//@formatter:on
			Map<String, BigInteger> report = wsp.reestablish(dbId, true);

			report.forEach((k, v) -> {

				if (v.doubleValue() > 0) {
					WorldStateService.setBalance(dbId, k, v);
				} else {
					System.err.println("found a negative number [" + k + "," + v + "],which is not allowed");
					WorldStateService.setBalance(dbId, k, BigInteger.ZERO);// eliminate minus number case.
				}

			});
		} else {
			System.out.println(
					"java -cp ./lfn-probe-0.5.0.jar:./localfullnode-2.0.0.jar one.inve.lfn.probe.ws.WorldStateReestablishment [dbId]");
			System.out.println("dbId: database id like \"0_1\" or \"0_6\"");
		}
	}

	public static void main1(String[] args) {
		final String dbId;
		final String command;

		if (args != null && args.length >= 2) {
			command = args[0];
			dbId = args[1];

			if (command.equals("genReport")) {
				WorldStateParadigm wsp = new WorldStateParadigm();

				Map<String, BigInteger> report = wsp.reestablish(dbId, false);

				System.out.println(report);
			} else if (command.equals("reestablishWorldState")) {
				RepositoryProvider.getTrack(dbId);

				WorldStateParadigm wsp = new WorldStateParadigm();
				Map<String, BigInteger> report = wsp.reestablish(dbId, false);

				report.forEach((k, v) -> {

					if (v.doubleValue() > 0) {
						WorldStateService.setBalance(dbId, k, v);
					} else {
						System.err.println("found a negative number [" + k + "," + v + "],which is not allowed");
					}

				});
			}
		} else {
			System.out.println(
					"java -cp ./lfn-probe-0.5.0.jar:./localfullnode-2.0.0.jar one.inve.lfn.probe.ws.WorldState [command] [dbId]");
			System.out.println("command:");
			System.out.println("	genReport - generate a report about address - final value");
			System.out.println("	reestablishWorldState - reestablish the whole World State");
			System.out.println("dbId: database id like \"0_1\"");
		}
	}

	public static void main0(String[] args) {
		WorldStateParadigm wsp = new WorldStateParadigm();

		Map<String, BigInteger> report = wsp.reestablish("0_1", false);

//		Map<String, BigInteger> report0 = new HashMap<>();
//		report.forEach((k, v) -> {
//			report0.put(k, v.divide(new BigInteger("1000000000000000000")));
//		});
//		System.out.println(report0);

		System.out.println(report);

//		Map<String, BigInteger> balance = new HashMap<>();
//		setBalance(balance, "LR4D5HXC4FBNM2RLNJIDTR63QXHIVQRO", "X5HT3ZFIOP2UK3ZK2X5TAQTSB6XFAZIY",
//				new BigInteger("66000000000000000000"));
//		setBalance(balance, "JCKNOS7KFSBZVRFYLJB7OSC35TQOEMF6", "X5HT3ZFIOP2UK3ZK2X5TAQTSB6XFAZIY",
//				new BigInteger("66000000000000000000"));
//		setBalance(balance, "FODALDEJMW5YIMUNPBCYB7P5QQPXQZ73", "X5HT3ZFIOP2UK3ZK2X5TAQTSB6XFAZIY",
//				new BigInteger("66000000000000000000"));
//		setBalance(balance, "OZGPDQBUUWC4H4XCOVOYKSQY32EX3GGO", "X5HT3ZFIOP2UK3ZK2X5TAQTSB6XFAZIY",
//				new BigInteger("66000000000000000000"));
//		setBalance(balance, "LR4D5HXC4FBNM2RLNJIDTR63QXHIVQRO", "X5HT3ZFIOP2UK3ZK2X5TAQTSB6XFAZIY",
//				new BigInteger("7000000000000000000"));
//		setBalance(balance, "X5HT3ZFIOP2UK3ZK2X5TAQTSB6XFAZIY", "6GTHF7OFAGTZ6HS6KHLD44HDUC6XCJMG",
//				new BigInteger("100000000000000000000"));
//		setBalance(balance, "LR4D5HXC4FBNM2RLNJIDTR63QXHIVQRO", "X5HT3ZFIOP2UK3ZK2X5TAQTSB6XFAZIY",
//				new BigInteger("666666000000000000000000"));
//		setBalance(balance, "X5HT3ZFIOP2UK3ZK2X5TAQTSB6XFAZIY", "6GTHF7OFAGTZ6HS6KHLD44HDUC6XCJMG",
//				new BigInteger("250000000000000000000"));
//		setBalance(balance, "X5HT3ZFIOP2UK3ZK2X5TAQTSB6XFAZIY", "6GTHF7OFAGTZ6HS6KHLD44HDUC6XCJMG",
//				new BigInteger("5000000000000000000000"));
//
//		System.out.println(balance.get("X5HT3ZFIOP2UK3ZK2X5TAQTSB6XFAZIY"));// 661587000000000000000000

	}

	private static void setBalance(Map<String, BigInteger> balance, String fromAddr, String toAddr, BigInteger value) {
		if (balance.get(fromAddr) == null)
			balance.put(fromAddr, new BigInteger("0"));
		if (balance.get(toAddr) == null)
			balance.put(toAddr, new BigInteger("0"));

		balance.put(fromAddr, balance.get(fromAddr).subtract(value));
		balance.put(toAddr, balance.get(toAddr).add(value));
	}

}
