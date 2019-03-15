package com.test.rocketmq.util;

import java.util.List;

import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;

public class RocketMQMessageListener implements MessageListenerConcurrently {

	private IMessageProcessor messageProcessor;

	public void setMessageProcessor(IMessageProcessor messageProcessor) {
		this.messageProcessor = messageProcessor;
	}

	@Override
	public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
			ConsumeConcurrentlyContext context) {
		try {
			for (MessageExt messageExt : msgs) {
				boolean result = this.messageProcessor
						.handleMessage(messageExt);
				if(!result){
					return ConsumeConcurrentlyStatus.RECONSUME_LATER;
				}
			}
			return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

}
