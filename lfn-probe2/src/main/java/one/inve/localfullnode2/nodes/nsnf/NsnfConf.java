package one.inve.localfullnode2.nodes.nsnf;

import java.util.List;
import java.util.Vector;

import one.inve.localfullnode2.lc.ILifecycle;

/**
 * 
 * 
 * Copyright © CHXX Co.,Ltd. All rights reserved.
 * 
 * @Description: map class to nsnf.toml
 * @author: Francis.Deng
 * @date: May 29, 2019 7:44:40 PM
 * @version: V1.0
 */
public class NsnfConf {
	private ILifecycle coreLC;
	private String shardingInfo;

	private List<LFN> localfullnodes = new Vector<>();

	private List<M> members = new Vector<>();

	public ConfigurableLocalfullnodes into() {
		return new ConfigurableLocalfullnodes(this);
	}

	public ILifecycle getCoreLC() {
		return coreLC;
	}

	public void setCoreLC(ILifecycle coreLC) {
		this.coreLC = coreLC;
	}

	public String getShardingInfo() {
		return shardingInfo;
	}

	public void setShardingInfo(String shardingInfo) {
		this.shardingInfo = shardingInfo;
	}

	public List<LFN> getLocalfullnodes() {
		return localfullnodes;
	}

	public void addLocalfullnode(LFN localfullnode) {
		this.localfullnodes.add(localfullnode);
	}

	public List<M> getMembers() {
		return members;
	}

	public void addMembers(M member) {
		this.members.add(member);
	}

	protected static class LFN {
		private String publicKey;
		private int status;
		private String shard;
		private String index;
		private String address;

		public String getPublicKey() {
			return publicKey;
		}

		public void setPublicKey(String publicKey) {
			this.publicKey = publicKey;
		}

		public int getStatus() {
			return status;
		}

		public void setStatus(int status) {
			this.status = status;
		}

		public String getShard() {
			return shard;
		}

		public void setShard(String shard) {
			this.shard = shard;
		}

		public String getIndex() {
			return index;
		}

		public void setIndex(String index) {
			this.index = index;
		}

		public String getAddress() {
			return address;
		}

		public void setAddress(String address) {
			this.address = address;
		}

	}

	protected static class M {
		private String level;
		private String rpcPort;
		private String index;
		private String address;
		private String id;

		public String getLevel() {
			return level;
		}

		public void setLevel(String level) {
			this.level = level;
		}

		public String getRpcPort() {
			return rpcPort;
		}

		public void setRpcPort(String rpcPort) {
			this.rpcPort = rpcPort;
		}

		public String getIndex() {
			return index;
		}

		public void setIndex(String index) {
			this.index = index;
		}

		public String getAddress() {
			return address;
		}

		public void setAddress(String address) {
			this.address = address;
		}

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

	}
}
