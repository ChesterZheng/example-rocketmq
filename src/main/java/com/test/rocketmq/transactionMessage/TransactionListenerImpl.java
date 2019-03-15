package com.test.rocketmq.transactionMessage;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

public class TransactionListenerImpl implements TransactionListener {

	private AtomicInteger transactionIndex = new AtomicInteger(0);

	private ConcurrentHashMap<String, Integer> localTransactionMap = new ConcurrentHashMap<String, Integer>();

	@Override
	public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
		int value = transactionIndex.getAndIncrement();
		int status = value % 3;
		localTransactionMap.put(msg.getTransactionId(), status);
		System.out.println("TransactionListener gets message body is " + new String(msg.getBody()));
		switch (status) {
		case 0:
			return LocalTransactionState.COMMIT_MESSAGE;
		case 1:
			return LocalTransactionState.ROLLBACK_MESSAGE;
		default:
			return LocalTransactionState.UNKNOW;
		}
	}

	@Override
	public LocalTransactionState checkLocalTransaction(MessageExt msg) {
		Integer status = localTransactionMap.get(msg.getTransactionId());
		if (null != status) {
			switch (status) {
			case 0:
				return LocalTransactionState.UNKNOW;
			case 1:
				return LocalTransactionState.COMMIT_MESSAGE;
			case 2:
				return LocalTransactionState.ROLLBACK_MESSAGE;
			}
		}
		return LocalTransactionState.COMMIT_MESSAGE;
	}

}
