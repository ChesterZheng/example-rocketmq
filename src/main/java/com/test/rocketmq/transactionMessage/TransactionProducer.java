package com.test.rocketmq.transactionMessage;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

public class TransactionProducer {

	public static void main(String[] args) throws MQClientException, InterruptedException {
		TransactionListener transactionListener = new TransactionListenerImpl();
		TransactionMQProducer producer = new TransactionMQProducer("test");
		ExecutorService executorService = new ThreadPoolExecutor(2, 5, 100, TimeUnit.SECONDS,
				new ArrayBlockingQueue<Runnable>(2000), new ThreadFactory() {
					@Override
					public Thread newThread(Runnable r) {
						Thread thread = new Thread(r);
						thread.setName("Test-RocketMQ-Transaction-Massage-Thread");
						return thread;
					}
				});
		producer.setNamesrvAddr("192.168.20.101:9876;192.168.20.102:9876");
		producer.setExecutorService(executorService);
		producer.setTransactionListener(transactionListener);
		producer.start();

		String[] tags = new String[] { "TagA", "TagB", "TagC", "TagD", "TagE" };
		Message msg = null;
		for (int i = 0; i < 10; i++) {
			try {
				msg = new Message("TopicTestTransactionMessage", tags[i % tags.length], "KEY" + i,
						("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
				SendResult sendResult = producer.sendMessageInTransaction(msg, null);
				System.out.printf("%s%n", sendResult);

				Thread.sleep(10);
			} catch (MQClientException | UnsupportedEncodingException e) {
				e.printStackTrace();
			}
		}

		producer.shutdown();
	}

}
