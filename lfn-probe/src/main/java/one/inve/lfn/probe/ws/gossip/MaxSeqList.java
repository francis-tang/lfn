package one.inve.lfn.probe.ws.gossip;

import com.alibaba.fastjson.JSON;

import one.inve.lfn.probe.common.cli.CliParser;
import one.inve.lfn.probe.common.cli.IntHolder;
import one.inve.lfn.probe.common.cli.StringHolder;
import one.inve.lfn.probe.core.gossip.GossipProtocol;
import one.inve.rpc.localfullnode.GossipObj;

/**
 * 
 * 
 * Copyright © CHXX Co.,Ltd. All rights reserved.
 * 
 * @Description: TODO
 * @author: Francis.Deng
 * @date: Apr 21, 2019 6:04:08 AM
 * @version: V1.0
 */
public class MaxSeqList {

	public static void main(String[] args) {
		IntHolder howManyshards = new IntHolder();
		IntHolder howManyNodesInOneShard = new IntHolder();
		IntHolder shardId = new IntHolder();
		StringHolder dbId = new StringHolder();
		StringHolder snapVersion = new StringHolder();
		StringHolder rSeqs = new StringHolder();

		StringBuilder type = new StringBuilder();

		CliParser parser = new CliParser(
				"java -cp ./lfn-probe-0.5.0.jar:./localfullnode-2.0.0.jar one.inve.lfn.probe.ws.gossip.MaxSeqList");
		parser.addOption("-db,--dbid %s #database id", dbId);
		parser.addOption("-sv,--snapVersion %s #snap version", snapVersion);
		parser.addOption("-s,--seqs %s #sequence", rSeqs);

		parser.matchAllArgs(args);

		try {
			long[] requesterSeqs = stringToLongArray(rSeqs.value);

			GossipProtocol gp = new GossipProtocol(1, 10, 0, dbId.value);
			GossipObj go = gp.getGossipObj(snapVersion.value, requesterSeqs);

			System.out.println(JSON.toJSONString(go));
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	private static long[] stringToLongArray(String s) {
		String[] ss = s.split(",");
		long[] longArray = new long[ss.length];

		for (int i = 0; i < ss.length; i++) {
			longArray[i] = Long.parseLong(ss[i]);
		}

		return longArray;
	}

}
