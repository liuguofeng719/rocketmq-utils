package com.smzc.util.rocketMQUtil.impl;


import com.smzc.util.rocketMQUtil.MQProducer;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.selector.SelectMessageQueueByHash;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created with IntelliJ IDEA.
 *
 * @author lgfcxx
 * @version 1.0
 * @desc
 * @createtime 2017/8/20 下午4:50
 * @see JDK 1.7
 **/
public class MQProducerImpl implements MQProducer {

    private static Logger log = LoggerFactory.getLogger(MQProducerImpl.class.getName());

    private DefaultMQProducer producer;
    private Lock lock = new ReentrantLock();
    private String namesrvAddr;
    private String producerGroup;

    public DefaultMQProducer producer() throws Exception {
        try {
            lock.lock();
            if (producer == null) {
                if (StringUtils.isBlank(producerGroup)) {
                    producer = new DefaultMQProducer();
                } else {
                    producer = new DefaultMQProducer(producerGroup);
                }

                if (StringUtils.isBlank(namesrvAddr)) {
                    throw new Exception("MQ服务器地址为空!");
                }
                producer.setNamesrvAddr(namesrvAddr);
                producer.start();
            }
        } catch (MQClientException mqex) {
            try {
                producer.shutdown();
            } catch (Exception ex) {
            }

            producer = null;
            log.error("启动MQ出错:" + mqex.getMessage(), mqex);
            throw new Exception("启动MQ出错!");
        } finally {
            lock.unlock();
        }

        return producer;
    }

    public void sendMsg(Message msg) throws Exception {
        try {
            SendResult sendResult = producer().send(msg);
            log.info("sendMsg = " + sendResult.toString());
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
            throw new Exception("操作MQ出错!");
        }
    }

    public void sendOrderMsg(Message msg, Integer order) throws Exception {
        try {
            SendResult sendResult = producer().send(msg, new MessageQueueSelector() {
                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                    Integer id = (Integer) arg;
                    int index = id.intValue() % mqs.size();
                    return mqs.get(index);
                }
            }, order);
            log.info("sendOrderMsg = " + sendResult.toString());
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
            throw new Exception("操作MQ出错！");
        }
    }

    public SendResult sendOrderMsg(Message msg, String order) throws Exception {
        try {
            SendResult sendResult = producer().send(msg, new SelectMessageQueueByHash(), order);
            log.info("sendOrderMsg by SelectMessageQueueByHash =" + sendResult.toString());
            return sendResult;
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
            throw new Exception("操作MQ出错！");
        }
    }

    public void shutdown() throws Exception {
        producer().shutdown();
    }

    public String getNamesrvAddr() {
        return this.namesrvAddr;
    }

    public void setNamesrvAddr(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
    }

    public String getProducerGroup() {
        return this.producerGroup;
    }

    public void setProducerGroup(String producerGroup) {
        this.producerGroup = producerGroup;
    }
}
