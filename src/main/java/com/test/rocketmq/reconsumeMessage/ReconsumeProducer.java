package com.test.rocketmq.reconsumeMessage;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * RocketMQ - Producer测试类
 * 
 * @Author ZhengXiaoChen
 * @Date 2018年2月9日下午5:30:27
 * @Tags
 */
public class ReconsumeProducer {

	private static DefaultMQProducer producer = null;

	public static void main(String[] args) {
		System.out.print("[----------]Start");
		try {
			producerStart();
			for (int i = 0; i < 4; i++) {
				sendMessage("zxc", "Hello RocketMQ-" + i);
			}
		} finally {
			producer.shutdown();
		}
		System.out.print("[----------]Succeed");
	}

	/**
	 * 
	 * @Author ZhengXiaoChen
	 * @Description 启动生成者
	 * @Date 2018年2月9日下午5:32:02
	 * @Tags @return
	 * @ReturnType boolean
	 */
	private static boolean producerStart() {
		/**
		 * Producer配置项
		 * 
		 * producerGroup DEFAULT_PRODUCER
		 * producer组名称，多个Producer如果属于一个应用，发送同样的消息，则应该将它们归为一组 createTopicKey
		 * TBW102 在发送消息时，自动创建服务器不存在的topic，需要指定key defaultTopicQueueNums 4
		 * 在发送消息时，自动创建服务器不存在的topic，默认创建的队列数量 sendMsgTimeout 10000
		 * 发送消息时的超时时间，单位：毫秒 compressMsgBodyOverHowmuch 4096
		 * 消息体超过指定大小后开始压缩（Consumer端收到消息后会自动解压），单位：字节 retryTimesWhenSendFailed
		 * 发送消息时，失败后的重试次数 retryAnotherBrokerWhenNotStoreOK FALSE
		 * 如果发送消息返回sendResult，但是sendStatus!=SEND_OK，是否重试发送 maxMessageSize 131072
		 * 客户端限制的消息大小，【超过则报错】，同时服务器也会限制（默认：128k） transactionCheckListener
		 * 事务消息回查监听器，如果发送事务消息，必须设置！！！ checkThreadPoolMinSize 1
		 * broker回查producer事务状态时，线程池的最小值 checkThreadPoolMaxSize 1
		 * broker回查producer事务状态时，线程池的最大值 checkRequestHoldMax 2000
		 * broker回查producer事务状态时，producer本地缓冲请求队列的大小
		 * 
		 */
		producer = new DefaultMQProducer("zxc_rocketmq_producer");
		// 设置NameServer地址
		producer.setNamesrvAddr("192.168.2.74:9876;192.168.2.75:9876");
		// 需要往不同的集群发消息时须设置instanceName
		// producer.setInstanceName(UUID.randomUUID().toString());
		// 发送消息失败时重试次数
		producer.setRetryTimesWhenSendFailed(3);
		producer.setVipChannelEnabled(false);
		//设置发送的消息内容超过指定阈值的时候进行压缩
		producer.setCompressMsgBodyOverHowmuch(32);
		try {
			producer.start();
		} catch (MQClientException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	/**
	 * 
	 * @Author ZhengXiaoChen
	 * @Description 发送消息
	 * @Date 2018年2月9日下午5:31:52
	 * @Tags @param topic
	 * @Tags @param str
	 * @Tags @return
	 * @ReturnType boolean
	 */
	private static boolean sendMessage(String topic, String str) {
		Message msg = new Message(topic, str.getBytes());
		try {

			producer.send(msg, new SendCallback() {
				// 发送成功时回调
				@Override
				public void onSuccess(SendResult sendResult) {
					System.out.println(sendResult);
					System.out.println("发送成功!");
				}

				// 发送失败时回调
				@Override
				public void onException(Throwable e) {
					System.out.println("发送失败! " + e.getMessage());
				}
			});
		} catch (MQClientException | RemotingException | InterruptedException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
}
