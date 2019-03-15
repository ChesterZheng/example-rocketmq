package com.test.rocketmq.normalPullMessage;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

public class NormalPullProducer {
	public static void main(String[] args) throws MQClientException,
			RemotingException, MQBrokerException, InterruptedException {
		String groupName = "transaction_producer";

		DefaultMQProducer producer = new DefaultMQProducer(groupName);
		// 设置NameServer地址
		producer.setNamesrvAddr("192.168.2.74:9876;192.168.2.75:9876");
		producer.start();
		for (int i = 0; i < 50; i++) {
			Message msg = new Message("test_pull_topic", "key",
					("RocketMQ-PullConsumer-" + i).getBytes());
			producer.send(msg);
		}
	}
}
