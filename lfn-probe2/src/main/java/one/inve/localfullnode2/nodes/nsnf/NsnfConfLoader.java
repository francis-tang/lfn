package one.inve.localfullnode2.nodes.nsnf;

import java.io.InputStream;
import java.util.List;

import com.moandjiezana.toml.Toml;

import one.inve.localfullnode2.nodes.nsnf.NsnfConf.LFN;
import one.inve.localfullnode2.nodes.nsnf.NsnfConf.M;
import one.inve.localfullnode2.utilities.ReflectionUtils;

/**
 * 
 * 
 * Copyright © CHXX Co.,Ltd. All rights reserved.
 * 
 * @Description: nsnf toml loader
 * @author: Francis.Deng
 * @date: May 29, 2019 7:44:05 PM
 * @version: V1.0
 */
public class NsnfConfLoader {
	public NsnfConf load(InputStream is) {
		// InputStream is = this.getClass().getResourceAsStream("nsnf.toml");

//		Toml toml = new Toml().read(is);
//		List<Toml> localfullnodes = toml.getTables("localfullnodes");
//
//		for (Toml localfullnode : localfullnodes) {
//			System.out.println(localfullnode.getString("pubkey"));
//			System.out.println(localfullnode.getLong("status"));
//			System.out.println(localfullnode.getString("shard"));
//			System.out.println(localfullnode.getString("index"));
//			System.out.println(localfullnode.getString("address"));
//		}

		NsnfConf conf = new NsnfConf();
		Toml toml = new Toml().read(is);

		String shardingInfo = toml.getString("sharding.info");
		conf.setShardingInfo(shardingInfo);

		String coreClassName = toml.getString("loopClassImpl");
		conf.setCoreLC(ReflectionUtils.getInstanceByClassName(coreClassName));

		loadLocalfullnodes(toml, conf);
		loadMembers(toml, conf);

		return conf;
	}

	private void loadMembers(Toml toml, NsnfConf conf) {
		List<Toml> members = toml.getTables("members");

		for (Toml member : members) {
			M m = new M();
			m.setLevel(member.getString("level"));
			m.setRpcPort(member.getString("rpcPort"));
			m.setIndex(member.getString("index"));
			m.setAddress(member.getString("address"));
			m.setId(member.getString("id"));

			conf.addMembers(m);
		}

	}

	private void loadLocalfullnodes(Toml toml, NsnfConf conf) {
		List<Toml> localfullnodes = toml.getTables("localfullnodes");

		for (Toml localfullnode : localfullnodes) {
			LFN lfn = new LFN();
			lfn.setPublicKey(localfullnode.getString("pubkey"));
			lfn.setStatus(localfullnode.getLong("status").intValue());
			lfn.setShard(localfullnode.getString("shard"));
			lfn.setIndex(localfullnode.getString("index"));
			lfn.setAddress(localfullnode.getString("address"));

			conf.addLocalfullnode(lfn);
		}
	}

//	public static void main(String[] args) {
//		NsnfConfLoader nsnfConfLoader = new NsnfConfLoader();
//		nsnfConfLoader.load();
//	}
}
