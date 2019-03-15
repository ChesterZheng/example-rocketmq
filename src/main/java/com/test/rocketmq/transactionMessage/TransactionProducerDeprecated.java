package com.test.rocketmq.transactionMessage;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.LocalTransactionExecuter;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionCheckListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

@Deprecated
public class TransactionProducerDeprecated {

	public static void main(String[] args) throws MQClientException {
		String groupName = "transaction_producer";

		final TransactionMQProducer producer = new TransactionMQProducer(
				groupName);
		// 设置NameServer地址
		producer.setNamesrvAddr("192.168.2.74:9876;192.168.2.75:9876");
		// 设置事务回查线程池最小值
		producer.setCheckThreadPoolMinSize(5);
		// 设置事务回查线程池最大值
		producer.setCheckThreadPoolMaxSize(20);
		// 队列数量
		producer.setCheckRequestHoldMax(2000);
		// 服务器回调Producer，检查本地事务分支成功还是失败
		producer.setTransactionCheckListener(new TransactionCheckListener() {
			@Override
			public LocalTransactionState checkLocalTransactionState(
					MessageExt msg) {
				System.out.println("statement -- " + new String(msg.getBody()));
				// 如果数据入库真实发生变化，则再次改变提交状态
				// 否则 数据库没有发生变化，则直接忽略该数据，然后回滚即可
				return LocalTransactionState.COMMIT_MESSAGE;
			}
		});
		producer.start();
		Message msg = new Message("test_topic", "Transaction", "key",
				("RocketMQ-TransactionMessage").getBytes());
		producer.sendMessageInTransaction(msg, new LocalTransactionExecuter() {
			@Override
			public LocalTransactionState executeLocalTransactionBranch(
					Message msg, Object arg) {
				System.out.println(msg);
				System.out.println(arg);
				//执行入库操作
				return LocalTransactionState.COMMIT_MESSAGE;
			}
		}, "RocketMQ");
	}
}
