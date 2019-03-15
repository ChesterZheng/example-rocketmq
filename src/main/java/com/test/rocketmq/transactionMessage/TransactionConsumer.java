package com.test.rocketmq.transactionMessage;

import java.util.List;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * RocketMQ事务消息的消费者示例
 * 此类消费者不关注Producer端是否是事务消息
 * 消费模型同DefaultMQConsumer
 * @Author ZhengXiaoChen
 * @Date 2018年3月5日下午3:06:14
 * @Tags
 */
public class TransactionConsumer {

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
			// 注册消费监听器
			consumer.registerMessageListener(new MessageListenerConcurrently() {
				@Override
				public ConsumeConcurrentlyStatus consumeMessage(
						List<MessageExt> list,
						ConsumeConcurrentlyContext consumeConcurrentlyContext) {
					try {
						for (MessageExt me : list) {
							System.out.println(me.getTopic());
							System.out.println(me.getTags());
							System.out.print(new String(me.getBody(), "UTF-8"));
						}
						return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
					} catch (Exception e) {
						e.printStackTrace();
						return ConsumeConcurrentlyStatus.RECONSUME_LATER;
					}
				}
			});
			consumer.start();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
