package com.test.rocketmq.util;

import org.apache.rocketmq.common.message.MessageExt;

public interface IMessageProcessor {
	//每次只处理一条消息
	public boolean handleMessage(MessageExt messageExt) throws Exception;
}
