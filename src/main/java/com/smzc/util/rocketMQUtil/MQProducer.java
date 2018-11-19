package com.smzc.util.rocketMQUtil;


import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

/**
 * Created with IntelliJ IDEA.
 *
 * @author lgfcxx
 * @version 1.0
 * @desc
 * @createtime 2017/8/20 下午4:48
 * @see JDK 1.7
 **/
public interface MQProducer {

    /**
     * 发送无序
     */
    void sendMsg(final Message msg);

    /**
     * 发送有序消息
     */
    SendResult sendOrderMsg(final Message msg, final Integer shardingKey);

    SendResult sendOrderMsg(final Message msg, final String shardingKey);

    /**
     * 异步发送消息
     */
    void sendAsync(final Message msg, final SendCallback sendCallback);

    void start() throws Exception;

    void shutdown() throws Exception;
}
