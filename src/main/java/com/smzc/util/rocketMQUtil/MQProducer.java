package com.smzc.util.rocketMQUtil;


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

    void sendMsg(Message msg) throws Exception;

    void sendOrderMsg(Message msg, Integer order) throws Exception;

    SendResult sendOrderMsg(Message msg, String order) throws Exception;

    void shutdown() throws Exception;
}
