package com.test.rocketmq.normalPullMessage;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

public class NormalPullConsumer {

	// key为指定的队列，value为这个队列拉取数据的最后位置
	private static final Map<MessageQueue, Long> offsetTable = new HashMap<MessageQueue, Long>();

	public static void main(String[] args) throws MQClientException {
		String groupName = "pullConsummer";
		DefaultMQPullConsumer consumer = new DefaultMQPullConsumer(groupName);
		/**
		 * PullConsumer配置
		 * 
		 * brokerSuspendMaxTimeMillis 20000
		 * 长轮询，Consumer拉取消息请求在Broker挂起的最长时间，单位：毫秒 consumerPullTimeoutMillis
		 * 10000 非长轮询，拉取消息超时时间，单位：毫秒 consumerTimeoutmillisWhenSuspend 30000 长轮询，
		 * Consumer拉取消息请求在Broker挂起超过指定时间，客户端认为超时，单位：毫秒 messageModel BROADCASTING
		 * 消息模型，支持两种：1.集群消费；2.广播消费 messageQueueListener 监听队列变化 offsetStore
		 * 消费进度存储 registerTopics 注册的topic集合 allocateMessageQueueStrategy
		 * 负载均衡算法实现策略
		 */
		consumer.setNamesrvAddr("192.168.2.74:9876;192.168.2.75:9876");
		consumer.start();
		// 从test_topic这个主题去获取所有的队列（默认是4个队列）
		Set<MessageQueue> msgQueues = consumer
				.fetchSubscribeMessageQueues("test_pull_topic");
		// 遍历每个队列，进行拉取数据
		for (MessageQueue mq : msgQueues) {
			System.out.println("Consumer from the queue：" + mq);
			SINGLE_MQ: while (true) {
				try {
					// 从queue中获取数据，从什么位置开始拉取数据，单词最多拉取32条记录
					PullResult pullResult = consumer.pullBlockIfNotFound(mq,
							null, getMessageQueueOffset(mq), 32);
					System.out.println(pullResult);
					System.out.println(pullResult.getPullStatus());
					System.out.println();
					putMessageQueueOffset(mq, pullResult.getNextBeginOffset());
					switch (pullResult.getPullStatus()) {
						case FOUND:
							List<MessageExt> list = pullResult.getMsgFoundList();
							for (MessageExt msg : list) {
								System.out.println(new String(msg.getBody()));
							}
							break;
						case NO_MATCHED_MSG:
							break;
						case NO_NEW_MSG:
							System.out.println("没有新消息了。。。");
							break SINGLE_MQ;
						case OFFSET_ILLEGAL:
							break;
						default:
							break;
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * 
	 * @Author ZhengXiaoChen
	 * @Description 获取消息消费的最新位置
	 * @Date 2018年3月5日下午3:51:09
	 * @Tags @param mq
	 * @Tags @return
	 * @Tags @throws Exception
	 * @ReturnType long
	 */
	private static long getMessageQueueOffset(MessageQueue mq) throws Exception {
		Long offset = offsetTable.get(mq);
		if (offset != null) {
			return offset;
		}
		return 0;
	}

	/**
	 * 
	 * @Author ZhengXiaoChen
	 * @Description 存储本次消费的最终位置
	 * @Date 2018年3月5日下午3:52:08
	 * @Tags @param mq
	 * @Tags @param offset
	 * @Tags @throws Exception
	 * @ReturnType void
	 */
	private static void putMessageQueueOffset(MessageQueue mq, long offset)
			throws Exception {
		offsetTable.put(mq, offset);
	}
}
