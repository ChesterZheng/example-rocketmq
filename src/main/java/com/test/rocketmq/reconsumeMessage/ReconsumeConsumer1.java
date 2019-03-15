package com.test.rocketmq.reconsumeMessage;

import java.util.List;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

/**
 * RocketMQ - Consumer测试类
 * 
 * @Author ZhengXiaoChen
 * @Date 2018年2月9日下午5:32:46
 * @Tags
 */
public class ReconsumeConsumer1 {

	public static void main(String[] args) {
		/**
		 * PushConsumer配置项
		 * 
		 * messageModel CLUSTERING 消息模型：集群消费；广播消费 consumeFromWhere
		 * CONSUME_FROM_LAST_OFFSET 消费端启动后，默认从什么位置开始消费
		 * allocateMessageQueueStrategy allocateMessageQueueAveragely
		 * Reblance算法实现策略 Subscripton 订阅关系 messageListener 消息监听器 offsetStore
		 * 消费进度存储 consumeThreadMin 10 消费线程池最小值 consumeThreadMax 20 消费线程池最大值
		 * consumeConcurrentlyMaxSpan 2000 单队列并行消费允许的最大跨度 pullThresholdForQueue
		 * 1000 拉消息本地队列缓存消息最大数量 Pullinterval
		 * 拉消息间隔，由于是长轮询，所以为0，但是如果应用了流控，也可以设置大于0的值，单位：毫秒
		 * consumeMessageBatchMaxSize 1 批量消费，一次消费多少条消息 pullBatchSize 32
		 * 批量消费，一次最多拉取多少条消息
		 */
		DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(
				"zxc-1");
		// consumer.setInstanceName(UUID.randomUUID().toString());
		consumer.setNamesrvAddr("192.168.2.74:9876;192.168.2.75:9876");
		try {
			// 订阅的tag支持的匹配格式：* 表示全部，TagA||TagB||TagC，CommandLog指定类型
			consumer.subscribe("zxc", "*");
			// 启动时从最新的位置开始消费
			consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
			// 每次消费消息的数量
			consumer.setConsumeMessageBatchMaxSize(1);
			// 设置消息模式(集群模式和广播模式)
			consumer.setMessageModel(MessageModel.CLUSTERING);
			// 注册消费监听器
			consumer.registerMessageListener(new MessageListenerConcurrently() {
				@Override
				public ConsumeConcurrentlyStatus consumeMessage(
						List<MessageExt> list,
						ConsumeConcurrentlyContext consumeConcurrentlyContext) {
					MessageExt messageExt = list.get(0);
					try {
						String topic = messageExt.getTopic();
						String tags = messageExt.getTags();
						String body = new String(messageExt.getBody(), "UTF-8");
						System.out.println("consumer1收到消息：" + " topic=" + topic
								+ ", tags=" + tags + ", msg=" + body);
						String originMsgId = messageExt.getProperties().get(
								MessageConst.PROPERTY_ORIGIN_MESSAGE_ID);
						System.out.println("consumer1 originMsgId=" + originMsgId);
						// Thread.sleep(20000);
//						int i = 1 / 0;
						return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
					} catch (Exception e) {
						if (messageExt.getReconsumeTimes() == 2) {
							System.out.println("记录日志");
							return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
						}
						e.printStackTrace();
						return ConsumeConcurrentlyStatus.RECONSUME_LATER;
					}
				}
			});
			consumer.start();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		DefaultMQPushConsumer consumer2 = new DefaultMQPushConsumer(
				"zxc-1");
		// consumer.setInstanceName(UUID.randomUUID().toString());
		consumer2.setNamesrvAddr("192.168.2.74:9876;192.168.2.75:9876");
		try {
			// 订阅的tag支持的匹配格式：* 表示全部，TagA||TagB||TagC，CommandLog指定类型
			consumer2.subscribe("zxc", "*");
			// 启动时从最新的位置开始消费
			consumer2.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
			// 每次消费消息的数量
			consumer2.setConsumeMessageBatchMaxSize(1);
			// 设置消息模式(集群模式和广播模式)
			consumer2.setMessageModel(MessageModel.CLUSTERING);
			// 注册消费监听器
			consumer2.registerMessageListener(new MessageListenerConcurrently() {
				@Override
				public ConsumeConcurrentlyStatus consumeMessage(
						List<MessageExt> list,
						ConsumeConcurrentlyContext consumeConcurrentlyContext) {
					MessageExt messageExt = list.get(0);
					try {
						String topic = messageExt.getTopic();
						String tags = messageExt.getTags();
						String body = new String(messageExt.getBody(), "UTF-8");
						System.out.println("consumer2收到消息：" + " topic=" + topic
								+ ", tags=" + tags + ", msg=" + body);
						String originMsgId = messageExt.getProperties().get(
								MessageConst.PROPERTY_ORIGIN_MESSAGE_ID);
						System.out.println("consumer2 originMsgId=" + originMsgId);
						// Thread.sleep(20000);
//						int i = 1 / 0;
						return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
					} catch (Exception e) {
						if (messageExt.getReconsumeTimes() == 2) {
							System.out.println("记录日志");
							return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
						}
						e.printStackTrace();
						return ConsumeConcurrentlyStatus.RECONSUME_LATER;
					}
				}
			});
			consumer2.start();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
