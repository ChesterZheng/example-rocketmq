package com.test.rocketmq.util;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;

public class Consumer {

	//name-server地址
	private String namesrvAddr;
	//消费者组名称
	private String consumerGroupName;
	//订阅的主题
	private String topic;
	//订阅主题的分类
	private String tag = "*";
	//消费者最小线程数
	private int consumeThreadMin = 20;
	//消费者最大线程数
	private int consumeThreadMax = 64;

	//处理消息的接口
	private IMessageProcessor messageProcessor;

	public void init() {
		DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(
				this.consumerGroupName);
		consumer.setNamesrvAddr(this.namesrvAddr);
		try {
			consumer.subscribe(this.topic, this.tag);
			consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
			consumer.setConsumeThreadMin(this.consumeThreadMin);
			consumer.setConsumeThreadMax(this.consumeThreadMax);
			RocketMQMessageListener listener = new RocketMQMessageListener();
			listener.setMessageProcessor(this.messageProcessor);
			consumer.setMessageListener(listener);
			consumer.start();
		} catch (MQClientException e) {
			e.printStackTrace();
		}
	}
}
