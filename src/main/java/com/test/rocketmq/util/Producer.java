package com.test.rocketmq.util;

import org.apache.rocketmq.client.producer.DefaultMQProducer;

public class Producer {
	
	//消息生成者
	private DefaultMQProducer producer;
	//生产者组名称
	private String producerGroupName;
	//name-server地址
	private String namesrvAddr;
	//单条消息最大值
	private int maxMessageSize = 1024 * 1024 * 4;

	public void init() {
		this.producer = new DefaultMQProducer(this.producerGroupName);
		this.producer.setNamesrvAddr(this.namesrvAddr);
		this.producer.setMaxMessageSize(this.maxMessageSize);
		try {
			this.producer.start();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
