package one.inve.lfn.probe.core.snapshot;

import java.math.BigInteger;
import java.util.HashMap;

import one.inve.bean.message.SnapshotMessage;
import one.inve.bean.message.SnapshotPoint;

public interface ISnapshotState {
	void setSnapshotMessage(SnapshotMessage snapshotMessage);

	HashMap<BigInteger, SnapshotPoint> getSnapshotPointMap();

	HashMap<BigInteger, String> getTreeRootMap();

	BigInteger getCurrSnapshotVersion();

	// default impl
	public static class Default implements ISnapshotState {

		private SnapshotMessage snapshotMessage = null;
		/** 最新快照点map < vers, SnapshotPoint > */
		private HashMap<BigInteger, SnapshotPoint> snapshotPointMap = new HashMap<>();
		/** 最新快照点版本的消息hash tree root map < vers, msgHashTreeRoot > */
		private HashMap<BigInteger, String> treeRootMap = new HashMap<>();

		@Override
		public void setSnapshotMessage(SnapshotMessage snapshotMessage) {
			this.snapshotMessage = snapshotMessage;
		}

		@Override
		public HashMap<BigInteger, SnapshotPoint> getSnapshotPointMap() {
			return snapshotPointMap;
		}

		@Override
		public HashMap<BigInteger, String> getTreeRootMap() {
			return treeRootMap;
		}

		@Override
		public BigInteger getCurrSnapshotVersion() {
			return (null == snapshotMessage) ? BigInteger.ONE : BigInteger.ONE.add(snapshotMessage.getSnapVersion());
		}

	}
}
