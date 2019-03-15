package com.test.rocketmq.normalPullMessage;

import java.util.List;

import org.apache.rocketmq.client.consumer.MQPullConsumer;
import org.apache.rocketmq.client.consumer.MQPullConsumerScheduleService;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullTaskCallback;
import org.apache.rocketmq.client.consumer.PullTaskContext;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

/**
 * RocketMQ消费拉取消息示例（使用阿里官方提供的ScheduleService） 
 * 推荐此种方式
 * @Author ZhengXiaoChen
 * @Date 2018年3月5日下午3:53:59
 * @Tags
 */
public class NormalPullConsumerByScheduleService {
	
	public static void main(String[] args) throws MQClientException {

		String groupName = "pullConsummer";

		final MQPullConsumerScheduleService scheduleService = new MQPullConsumerScheduleService(
				groupName);

		scheduleService.getDefaultMQPullConsumer().setNamesrvAddr(
				"192.168.2.74:9876;192.168.2.75:9876");

		scheduleService.setMessageModel(MessageModel.CLUSTERING);

		scheduleService.registerPullTaskCallback("test_pull_topic",
				new PullTaskCallback() {
					@Override
					public void doPullTask(MessageQueue mq,
							PullTaskContext context) {
						MQPullConsumer consumer = context.getPullConsumer();
						try {
							// 获取offset值
							long offset = consumer
									.fetchConsumeOffset(mq, false);
							if (offset < 0) {
								offset = 0;
							}
							PullResult pullResult = consumer.pull(mq, "*",
									offset, 32);
							switch (pullResult.getPullStatus()) {
								case FOUND:
									List<MessageExt> list = pullResult
											.getMsgFoundList();
									for (MessageExt msg : list) {
										System.out.println(new String(msg.getBody()));
									}
									break;
								case NO_MATCHED_MSG:
									break;
								case NO_NEW_MSG:
									System.out.println("没有新消息了。。。");
									break;
								case OFFSET_ILLEGAL:
									break;
								default:
									break;
							}
							// 存储offset值，此方法会每隔5秒与Broker进行同步
							consumer.updateConsumeOffset(mq,
									pullResult.getNextBeginOffset());
							// 设置每次拉取数据的时间间隔
							context.setPullNextDelayTimeMillis(3000);
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				});
		scheduleService.start();
	}
}
