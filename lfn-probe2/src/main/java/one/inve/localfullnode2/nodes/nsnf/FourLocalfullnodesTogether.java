package one.inve.localfullnode2.nodes.nsnf;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import one.inve.bean.message.SnapshotMessage;
import one.inve.bean.node.LocalFullNode;
import one.inve.bean.node.NodeStatus;
import one.inve.cluster.Member;
import one.inve.localfullnode2.dep.DepItemsManager;
import one.inve.localfullnode2.hashnet.Hashneter;
import one.inve.localfullnode2.lc.ILifecycle;
import one.inve.localfullnode2.lc.LazyLifecycle;
import one.inve.localfullnode2.nodes.HashneterInitializer;
import one.inve.localfullnode2.nodes.LocalFullNode1GeneralNode;
import one.inve.localfullnode2.utilities.ReflectionUtils;
import one.inve.transport.Address;

/**
 * 
 * 
 * Copyright © CHXX Co.,Ltd. All rights reserved.
 * 
 * @Description: nsnf or LocalfullnodeAlone (which means
 *               "non-seed,non-fullnode,localfullnode alone network") allows
 *               coder to run localfullnodes without seed and fullnode in place
 *               facilitating testing and troubleshooting. Introduce system
 *               property of {@code "loop.classImpl"}
 * @author: Francis.Deng
 * @date: May 15, 2019 2:04:22 AM
 * @version: V1.0
 */
public class FourLocalfullnodesTogether extends HashneterInitializer {

	protected String shardId_creatorId_shardCount_nValue_address = "0_0_1_4";
	protected Address me;

	private String loopClassImpl = "one.inve.localfullnode2.lc.FormalEventMessageLoop";// standardized execution loop

	@Override
	public void asLocalFullNode(String seedPubIP, String seedRpcPort) {
		// do nothing at all

		// apart from these
		SnapshotMessage snapshotMessage = new SnapshotMessage();
		snapshotMessage.setSnapVersion(BigInteger.valueOf(1l));
		DepItemsManager.getInstance().attachUpdatedSnapshotMessage(null).set(snapshotMessage);

	}

	@Override
	public void shardInfo(String seedPubIP, String seedRpcPort) {
		String meta = System.getProperty("node.meta");
		if (meta != null) {
			setMeta(meta);
		}

		// setShardId,setCreatorId,setShardCount,setnValue
//		setShardId(0);
//		setCreatorId(1);
//		setShardCount(1);
//		setnValue(4);

		if (shardId_creatorId_shardCount_nValue_address != null) {
			String[] parts = shardId_creatorId_shardCount_nValue_address.split("_");

			setShardId(Integer.parseInt(parts[0]));
			setCreatorId(Integer.parseInt(parts[1]));
			setShardCount(Integer.parseInt(parts[2]));
			setnValue(Integer.parseInt(parts[3]));

			me = Address.from(parts[4]);
		}

	}

	public void setMeta(String meta) {
		this.shardId_creatorId_shardCount_nValue_address = meta;
	}

	@Override
	public void allLocalFullNodeList(String seedPubIP, String seedRpcPort) {

		// the pk and localfullnode info must fit into your environment.
		String pk0 = "MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCEVPUEsXu6nf2CymIn3/AnCpAOURwys0A4Y3j3yJ5TPy9JlFEiYiKp37spHYwCPXyhg2b7nxxoB/Mwr3qoed6pjJ95FZ3eczYxP5jnBNE0ZnTm2ZzGCvo0SimgCGKeK8KQ7u1qGIWlfExA6mg2pf50lp+dBnTQqDskv6lYr7zXmwIDAQAB";
		String pk1 = "MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCy3Ok+WOYiFvV1iTgeAdp5QI9mL7SVLCVcF5dSKXb6kBWpefWxLe4epk5kBosk3ddju0JIvKSkn2UrOd6b1F1Ditu+hptfkAJpnOd6DJIyQ3ev9YTdjYruAE+YRdsV/DOWN1wMJ6iIkzu+ERdRdyZnLyWqH4XfEfjDMuX2RqM7owIDAQAB";
		String pk2 = "MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCXsbhYmzRnrV7zKv2mjE7pcv5GyRGf8bCOBXZ1HwYqYq+1yWGlP9/HNlxr80PR1BMHTVl+QhwvBq+TRbHIgB5Qb3Q+2byLycNnUiF+AQ16bWH8XjYH4WXXoXN8OoZ/Dca1qoC6CYDoQQDVfljJf6JnTmotU5Ij69jbkfzybbEiQwIDAQAB";
		String pk3 = "MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCKUj9kEkBNuEjdjf1gXhgFOC+ZuOHWASgE1sg/gsNGLjarajKWFKUuhCKTb+bEE+N5/GoKQPWmdureLFw8kLnftBYgS7nwuH5cW8AE/l+bscbvIFn8lcE6QbQnwww3UDyby6pQ8exPuGsj1OAVzmh1+FJhVmeoFo5Bnnyd+HUqzwIDAQAB";

		LocalFullNode lfn0 = new LocalFullNode.Builder().pubkey(pk0).status(NodeStatus.HAS_SHARDED).shard("0")
				.index("0").address("address0").build();
		LocalFullNode lfn1 = new LocalFullNode.Builder().pubkey(pk1).status(NodeStatus.HAS_SHARDED).shard("0")
				.index("1").address("address1").build();
		LocalFullNode lfn2 = new LocalFullNode.Builder().pubkey(pk2).status(NodeStatus.HAS_SHARDED).shard("0")
				.index("2").address("address2").build();
		LocalFullNode lfn3 = new LocalFullNode.Builder().pubkey(pk3).status(NodeStatus.HAS_SHARDED).shard("0")
				.index("3").address("address3").build();

		List<LocalFullNode> localFullNodes = new LinkedList<LocalFullNode>() {
			{
				add(lfn0);
				add(lfn1);
				add(lfn2);
				add(lfn3);
			}
		};

		setLocalFullNodes(localFullNodes);

	}

	public static void main(String[] args) {
		FourLocalfullnodesTogether fourLocalfullnodesTogether = new FourLocalfullnodesTogether();
		fourLocalfullnodesTogether.start(args);
	}

	/**
	 * support system property of "Dloop.classImpl" for specified execution loop
	 */
	@Override
	protected ILifecycle performCoreTasks(Hashneter hashneter) {
		// ILifecycle lc = new FormalEventMessageLoop();
		ILifecycle lc = getLifecycle();
		lc.start();

		return lc;
	}

	@Override
	protected ILifecycle startMembership(LocalFullNode1GeneralNode node) {
		LazyLifecycle llc = new LazyLifecycle() {

			@Override
			public void start() {
				super.start();

				List<Member> members = new LinkedList<>();

//				.port(node.nodeParameters().selfGossipAddress.getGossipPort()).portAutoIncrement(false)
//				.addMetadata("level", "" + NodeTypes.LOCALFULLNODE)
//				.addMetadata("shard", node.getShardId() < 0 ? "" : "" + this.node.getShardId())
//				.addMetadata("index", node.getCreatorId() < 0 ? "" : "" + this.node.getCreatorId())
//				.addMetadata("rpcPort", "" + node.nodeParameters().selfGossipAddress.getRpcPort())
//				.addMetadata("httpPort", "" + node.nodeParameters().selfGossipAddress.getHttpPort())
//				.addMetadata("pubkey", this.pubkey).addMetadata("address", this.node.getWallet().getAddress()).build();	

				Map<String, String> meta0 = new HashMap<>();
				meta0.put("level", "2");
				meta0.put("rpcPort", "35792");
				meta0.put("index", "0");
				Address address0 = Address.create("192.168.207.129", 35791);
				if (!address0.equals(me))
					members.add(new Member("0", address0, meta0));
				// Member m0 = new Member("0", address0, meta0);

				Map<String, String> meta1 = new HashMap<>();
				meta1.put("level", "2");
				meta1.put("rpcPort", "35795");
				meta1.put("index", "1");
				Address address1 = Address.create("192.168.207.129", 35794);
				if (!address1.equals(me))
					members.add(new Member("1", address1, meta1));
				// Member m1 = new Member("1", address1, meta1);

				Map<String, String> meta2 = new HashMap<>();
				meta2.put("level", "2");
				meta2.put("rpcPort", "35798");
				meta2.put("index", "2");
				Address address2 = Address.create("192.168.207.129", 35797);
				if (!address2.equals(me))
					members.add(new Member("2", address2, meta2));
				// Member m2 = new Member("2", address2, meta2);

				Map<String, String> meta3 = new HashMap<>();
				meta3.put("level", "2");
				meta3.put("rpcPort", "35892");
				meta3.put("index", "3");
				Address address3 = Address.create("192.168.207.129", 35891);
				if (!address3.equals(me))
					members.add(new Member("3", address3, meta3));
				// Member m3 = new Member("3", address3, meta3);

				inshardNeighborPools(members);
			}

		};
		llc.start();

		return llc;
	}

	public void setLoopClassImpl(String loopClassImpl) {
		this.loopClassImpl = loopClassImpl;
	}

	protected ILifecycle getLifecycle() {
		String clazzName = System.getProperty("loop.classImpl");
		if (clazzName == null)
			clazzName = this.loopClassImpl;

		return ReflectionUtils.getInstanceByClassName(clazzName);
	}

//	private void sleep(int seconds) {
//		try {
//			TimeUnit.SECONDS.sleep(seconds);
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	}

}
