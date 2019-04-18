package one.inve.lfn.probe.ws;

import java.math.BigInteger;
import java.util.Map;

import one.inve.lfn.probe.core.tx.WorldStateParadigm;

/**
 * 
 * 
 * Copyright © CHXX Co.,Ltd. All rights reserved.
 * 
 * @Description: verbose output tool of world state before executing world state
 *               re-establishment.
 * @author: Francis.Deng
 * @date: Apr 10, 2019 1:44:10 AM
 * @version: V1.0
 */
public class VerboseWorldState {

	public static void main(String[] args) {
		final String dbId;

		if (args != null && args.length >= 1) {
			dbId = args[0];

			WorldStateParadigm wsp = new WorldStateParadigm();

			Map<String, BigInteger> report = wsp.reestablish(dbId, false);

			System.out.println(report);// make a comparison with block chain explorer.
		} else {
			System.out.println(
					"java -cp ./lfn-probe-0.5.0.jar:./localfullnode-2.0.0.jar one.inve.lfn.probe.ws.VerboseWorldState [dbId]");
			System.out.println("dbId: database id like \"0_1\"");
		}
	}

}
