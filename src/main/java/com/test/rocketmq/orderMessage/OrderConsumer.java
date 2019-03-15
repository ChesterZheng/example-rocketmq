package com.test.rocketmq.orderMessage;

import java.util.List;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * RocketMQ顺序消息的消费者示例
 * 
 * @Author ZhengXiaoChen
 * @Date 2018年3月2日下午5:41:33
 * @Tags
 */
public class OrderConsumer {

	public static void main(String[] args) {
		DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(
				"con_qch_test");
		// consumer.setInstanceName(UUID.randomUUID().toString());
		consumer.setNamesrvAddr("192.168.2.74:9876;192.168.2.75:9876");
		try {
			// 订阅的tag支持的匹配格式：* 表示全部，TagA||TagB||TagC，CommandLog指定类型
			consumer.subscribe("test_topic", "*");
			// 启动时从最新的位置开始消费
			consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
			// 每次消费消息的数量
			consumer.setConsumeMessageBatchMaxSize(32);
			// 注册消费监听器(实现MessageListenerOrderly接口，来保证消费消息为顺序的)
			consumer.registerMessageListener(new MessageListenerOrderly() {
				@Override
				public ConsumeOrderlyStatus consumeMessage(
						List<MessageExt> msgs, ConsumeOrderlyContext context) {
					context.setAutoCommit(true);
					try {
						for (MessageExt me : msgs) {
							System.out.println(me.getTopic());
							System.out.println(me.getTags());
							System.out.println(new String(me.getBody(), "UTF-8"));
						}
						return ConsumeOrderlyStatus.SUCCESS;
					} catch (Exception e) {
						e.printStackTrace();
						return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
					}
				}

			});
			consumer.start();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
