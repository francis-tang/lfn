package one.inve.lfn.probe.http;

import java.math.BigInteger;

import com.alibaba.fastjson.JSON;

import one.inve.beans.dao.TransactionArray;
import one.inve.db.transaction.MysqlHelper;
import one.inve.db.transaction.QueryTableSplit;
import one.inve.lfn.probe.common.cli.CliParser;
import one.inve.lfn.probe.common.cli.LongHolder;
import one.inve.lfn.probe.common.cli.StringHolder;

/**
 * 
 * 
 * Copyright © CHXX Co.,Ltd. All rights reserved.
 * 
 * @formatter:off
 * @Description: test underlying code because there is a report that some messages are missing when passing in 
 * { "tableIndex":0, "offset":170,"address":"EVPBGL2SN4Z44AIRZEIKBWUGQG5BXWQQ"}
 * 
 * run shell command to troubleshoot: 
 * java -cp ./lfn-probe-0.5.0.jar:./localfullnode-2.0.0.jar one.inve.lfn.probe.http.GetTransactionList -db 0_5 -ti 0 -o 170 -a EVPBGL2SN4Z44AIRZEIKBWUGQG5BXWQQ
 * 
 * @formatter:on
 * @author: Francis.Deng
 * @date: 2019年4月17日 下午4:19:34
 * @version: V1.0
 *           {@link one.inve.http.service.HttpApiService::getTransactionList}
 */
public class GetTransactionList {

	public static void main(String[] args) {
		StringHolder dbId = new StringHolder();
		LongHolder tableIndex = new LongHolder();
		LongHolder offset = new LongHolder();
		StringHolder address = new StringHolder();

		StringBuilder type = new StringBuilder();

		CliParser parser = new CliParser(
				"java -cp ./lfn-probe-0.5.0.jar:./localfullnode-2.0.0.jar one.inve.lfn.probe.http.GetTransactionList");
		parser.addOption("-db,--dbid %s #database id", dbId);
		parser.addOption("-ti,--tableindex %i {[0,800]} #messages table index", tableIndex);
		parser.addOption("-o,--offset %i #offset {[0,80000]} limitation", offset);
		parser.addOption("-a,--address %s #from address or to address", address);

		parser.matchAllArgs(args);

		try {
			MysqlHelper mh = new MysqlHelper(dbId.value, false);

			QueryTableSplit queryTableSplit = new QueryTableSplit();
			TransactionArray ta = queryTableSplit.queryTransaction(BigInteger.valueOf(tableIndex.value),
					Long.valueOf(offset.value), address.value, type.toString(), dbId.value);

			System.out.println(JSON.toJSONString(ta));
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
