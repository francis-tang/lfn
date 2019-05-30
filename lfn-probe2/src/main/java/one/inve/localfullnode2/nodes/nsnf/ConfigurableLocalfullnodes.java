package one.inve.localfullnode2.nodes.nsnf;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import one.inve.bean.message.SnapshotMessage;
import one.inve.bean.node.LocalFullNode;
import one.inve.cluster.Member;
import one.inve.localfullnode2.dep.DepItemsManager;
import one.inve.localfullnode2.hashnet.Hashneter;
import one.inve.localfullnode2.lc.ILifecycle;
import one.inve.localfullnode2.lc.LazyLifecycle;
import one.inve.localfullnode2.nodes.HashneterInitializer;
import one.inve.localfullnode2.nodes.LocalFullNode1GeneralNode;
import one.inve.transport.Address;

public class ConfigurableLocalfullnodes extends HashneterInitializer {

//	@Override
//	public void asLocalFullNode(String seedPubIP, String seedRpcPort) {
//		// TODO Auto-generated method stub
//
//	}

	private NsnfConf conf;
	protected Address me;

	public ConfigurableLocalfullnodes(NsnfConf conf) {
		this.conf = conf;
	}

	@Override
	public void shardInfo(String seedPubIP, String seedRpcPort) {
		if (conf.getShardingInfo() != null) {
			String[] parts = conf.getShardingInfo().split("_");

			setShardId(Integer.parseInt(parts[0]));
			setCreatorId(Integer.parseInt(parts[1]));
			setShardCount(Integer.parseInt(parts[2]));
			setnValue(Integer.parseInt(parts[3]));

			me = Address.from(parts[4]);
		}

	}

	@Override
	public void allLocalFullNodeList(String seedPubIP, String seedRpcPort) {
		// List<LocalFullNode> localFullNodes = new Vector<>();

		List<LocalFullNode> localFullNodes = conf
				.getLocalfullnodes().stream().map(t -> new LocalFullNode.Builder().pubkey(t.getPublicKey())
						.status(t.getStatus()).shard(t.getShard()).index(t.getIndex()).address(t.getAddress()).build())
				.collect(Collectors.toList());

		setLocalFullNodes(localFullNodes);

	}

	@Override
	protected ILifecycle startMembership(LocalFullNode1GeneralNode node) {
		LazyLifecycle llc = new LazyLifecycle() {

			@Override
			public void start() {
				super.start();

				List<Member> members = conf.getMembers().stream().filter(e -> {
					Address a = Address.from(e.getAddress());
					return !me.equals(a);
				}).map(e -> {
					Map<String, String> meta = new HashMap<>();

					meta.put("level", e.getLevel());
					meta.put("rpcPort", e.getRpcPort());
					meta.put("index", e.getIndex());

					Address address = Address.from(e.getAddress());

					return new Member(e.getId(), address, meta);
				}).collect(Collectors.toList());

				inshardNeighborPools(members);
			}

		};
		llc.start();

		return llc;
	}

	@Override
	public void asLocalFullNode(String seedPubIP, String seedRpcPort) {
		// do nothing at all

		// apart from these
		SnapshotMessage snapshotMessage = new SnapshotMessage();
		snapshotMessage.setSnapVersion(BigInteger.valueOf(1l));
		DepItemsManager.getInstance().attachUpdatedSnapshotMessage(null).set(snapshotMessage);

	}

	@Override
	protected ILifecycle performCoreTasks(Hashneter hashneter) {
		ILifecycle lc = conf.getCoreLC();
		lc.start();

		return lc;
	}

	public static void main(String[] args) throws FileNotFoundException {
		setSystemProperties(args);
		String nsnfConf = System.getProperty("nsnf.conf");

		if (nsnfConf != null && nsnfConf.length() > 0) {
			NsnfConfLoader loader = new NsnfConfLoader();
//			ConfigurableLocalfullnodes fourLocalfullnodesTogether = loader
//					.load(loader.getClass().getResourceAsStream(nsnfConf)).into();
			ConfigurableLocalfullnodes fourLocalfullnodesTogether = loader.load(new FileInputStream(new File(nsnfConf)))
					.into();

			fourLocalfullnodesTogether.start(args);
		}

	}

	private static void setSystemProperties(String[] args) {
		for (int i = 0; i < args.length; i++) {
			String p = args[i];
			if (p.startsWith("-D")) {
				p = p.substring(2);
				String[] keyValuePair = p.split("=");

				System.setProperty(keyValuePair[0].toLowerCase(), keyValuePair[1]);
			}
		}
	}

}
