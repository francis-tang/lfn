package one.inve.lfn.probe.ws;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.alibaba.fastjson.JSONArray;

import one.inve.bean.message.TransactionMessage;
import one.inve.beans.dao.Message;
import one.inve.db.transaction.MysqlHelper;
import one.inve.lfn.probe.core.tx.TxStore;

/**
 * 
 * 
 * Copyright © CHXX Co.,Ltd. All rights reserved.
 * 
 * @Description: verbose output tool of specified tx for troubleshooting.
 *               re-establishment.
 * @author: Francis.Deng
 * @date: Apr 10, 2019 1:44:10 AM
 * @version: V1.0
 */
public class VerboseTx {

	public static void main(String[] args) {

		String dbId;
		String interestingAddress;
		BigInteger o = new BigInteger("0");

		if (args.length >= 2) {
			dbId = args[0];
			interestingAddress = args[1];

			MysqlHelper mh = new MysqlHelper(dbId, false);

			TxStore txStore = new TxStore();
			List<Message> messages = txStore.getMessages(dbId, o, interestingAddress);

			// filter valid messages
			List<Message> validMessages = messages.stream().filter(m -> m.isValid()).collect(Collectors.toList());

			// verbose output
			validMessages.forEach((m) -> {
				String sMessage = m.getMessage();
				TransactionMessage tx = JSONArray.parseObject(sMessage, TransactionMessage.class);
				BigInteger fee = tx.getFee().multiply(tx.getNrgPrice());

				if (tx.getFromAddress().equals(interestingAddress)) {
					System.out.println("-" + tx.getAmount());
				} else if (tx.getToAddress().equals(interestingAddress)) {
					System.out.println("+" + tx.getAmount());
				}

				if (fee.compareTo(BigInteger.ZERO) > 0) {
					System.out.println("Fee " + fee);

				}

			});

			// final value
			Optional<BigInteger> finalV = validMessages.parallelStream().map((m) -> {
				String sMessage = m.getMessage();
				TransactionMessage tx = JSONArray.parseObject(sMessage, TransactionMessage.class);
				BigInteger fee = tx.getFee().multiply(tx.getNrgPrice());
				BigInteger result;

				if (tx.getFromAddress().equals(interestingAddress)) {
					result = tx.getAmount().negate();
				} else {
					result = tx.getAmount();
				}

				if (fee.compareTo(BigInteger.ZERO) > 0 && tx.getFromAddress().equals(interestingAddress)) {
					result.subtract(fee);
				}

				return result;
			}).reduce((a, b) -> a.add(b));

			System.out.println("final v=" + finalV);
		} else {
			System.out.println(
					"java -cp ./lfn-probe-0.5.0.jar:./localfullnode-2.0.0.jar one.inve.lfn.probe.ws.VerboseTx [dbId] [address]");
			System.out.println("dbId: database id like \"0_1\" or \"0_6\"");
			System.out.println("address: wallet address(either fromAddress or toAddress)");
		}

	}

}
