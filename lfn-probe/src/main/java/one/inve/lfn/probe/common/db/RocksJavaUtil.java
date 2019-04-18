package one.inve.lfn.probe.common.db;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import one.inve.util.PathUtils;

/**
 * 
 * 
 * Copyright © CHXX Co.,Ltd. All rights reserved.
 * 
 * @Description: a replacement of
 *               <code>one.inve.rocksDB.RocksJavaUtil</code>,with the
 *               enhancement in custom rocksdb file directory
 * @author: Francis.Deng
 * @date: 2019年4月8日 下午4:56:01
 * @version: V1.0
 */
public class RocksJavaUtil {
	private static final Logger logger = LoggerFactory.getLogger("rocksdb");
	private static String dbPath = PathUtils.getDataFileDir();
	private RocksDB rocksDB;
	public static Map<String, RocksDB> rockSDBMap = new HashMap<>();

	public RocksJavaUtil(String dbId, String rocksdbPath) {
		try {
			rocksDB = rockSDBMap.get(dbId);
			if (rocksDB == null) {

				String rocksDBPath = rocksdbPath + dbId;
				RocksDB.loadLibrary();

				List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
				Options options = new Options();
				options.setCreateIfMissing(true);

				List<byte[]> cfs = RocksDB.listColumnFamilies(options, rocksDBPath);
				if (cfs.size() > 0) {
					for (byte[] cf : cfs) {
						columnFamilyDescriptors.add(new ColumnFamilyDescriptor(cf, new ColumnFamilyOptions()));
					}
				} else {
					columnFamilyDescriptors
							.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, new ColumnFamilyOptions()));
				}

				List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
				DBOptions dbOptions = new DBOptions();
				dbOptions.setCreateIfMissing(true);

				rocksDB = RocksDB.open(dbOptions, rocksDBPath, columnFamilyDescriptors, columnFamilyHandles);
				rockSDBMap.put(dbId, rocksDB);
			}
		} catch (Exception ex) {
			logger.error(ex.getMessage(), ex);
		}
	}

	public RocksJavaUtil(String dbId) {

		try {
			rocksDB = rockSDBMap.get(dbId);
			if (rocksDB == null) {

				String rocksDBPath = dbPath + dbId;
				RocksDB.loadLibrary();

				List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
				Options options = new Options();
				options.setCreateIfMissing(true);

				List<byte[]> cfs = RocksDB.listColumnFamilies(options, rocksDBPath);
				if (cfs.size() > 0) {
					for (byte[] cf : cfs) {
						columnFamilyDescriptors.add(new ColumnFamilyDescriptor(cf, new ColumnFamilyOptions()));
					}
				} else {
					columnFamilyDescriptors
							.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, new ColumnFamilyOptions()));
				}

				List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
				DBOptions dbOptions = new DBOptions();
				// dbOptions.setCreateIfMissing(true);
				dbOptions.setCreateIfMissing(false);

				rocksDB = RocksDB.open(dbOptions, rocksDBPath, columnFamilyDescriptors, columnFamilyHandles);
				rockSDBMap.put(dbId, rocksDB);
			}
		} catch (Exception ex) {
			logger.error(ex.getMessage(), ex);
		}
	}

	public void set(String key) {
		try {
			rocksDB.put(key.getBytes(), key.getBytes());

		} catch (Exception ex) {
			logger.error("rocksDB.put异常", ex);
		}
	}

	public void put(String key, String value) {
		try {
			rocksDB.put(key.getBytes(), value.getBytes());

		} catch (Exception ex) {
			logger.error("rocksDB.put异常", ex);
		}
	}

	public void put(String key, byte[] value) {
		try {
			rocksDB.put(key.getBytes(), value);

		} catch (Exception ex) {
			logger.error("rocksDB.put异常", ex);
		}
	}

	public byte[] get(String key) {
		try {

			return rocksDB.get(key.getBytes());
		} catch (Exception ex) {
			logger.error("rocksDB.get异常", ex);
		}
		return null;
	}

	public void delete(String key) {
		try {
			rocksDB.delete(key.getBytes());
		} catch (Exception ex) {
			logger.error("rocksDB.delete异常", ex);
		}
	}

}
