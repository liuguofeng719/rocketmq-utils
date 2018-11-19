package com.smzc.util.rocketMQUtil.impl;

import com.smzc.util.rocketMQUtil.MQException;
import com.smzc.util.rocketMQUtil.MQProducer;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.selector.SelectMessageQueueByHash;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
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
 * @desc 生产者对象
 * @createtime 2017/8/20 下午4:50
 * @see JDK 1.7
 **/
public class MQProducerImpl implements MQProducer {

    private static Logger log = LoggerFactory.getLogger(MQProducerImpl.class.getName());

    private Lock lock = new ReentrantLock();

    private DefaultMQProducer defaultMQProducer;
    /**
     * nameSrv地址
     */
    private String namesrvAddr;
    /**
     * 分组名字
     */
    private String producerGroup;

    /**
     * 实例名字，通过实例名字通过工厂来获取MQClientInstance,
     * 默认值DEFAULT,如果需要连接多少rocketmq，需要设置不同的实例名称
     */
    private String instanceName;
    /**
     * 获取生产者对象
     * @return
     * @throws Exception
     */
    public void start() throws Exception {
        try {
            lock.lock();
            if (defaultMQProducer == null) {
                if (StringUtils.isBlank(producerGroup)) {
                    defaultMQProducer = new DefaultMQProducer();
                } else {
                    defaultMQProducer = new DefaultMQProducer(producerGroup);
                }

                if (StringUtils.isNotBlank(instanceName)) {
                    defaultMQProducer.setInstanceName(instanceName);
                }

                if (StringUtils.isBlank(namesrvAddr)) {
                    throw new Exception("MQ服务器地址为空!");
                }

                defaultMQProducer.setNamesrvAddr(namesrvAddr);
                defaultMQProducer.start();
            }
        } catch (MQClientException mqex) {
            try {
                defaultMQProducer.shutdown();
            } catch (Exception ex) {
            }
            defaultMQProducer = null;
            log.error("启动MQ出错:" + mqex.getMessage(), mqex);
            throw new Exception("启动MQ出错!");
        } finally {
            lock.unlock();
        }
    }

    /**
     * 发送无序消息
     * @param msg
     * @throws Exception
     */
    public void sendMsg(Message msg) {
        try {
            SendResult sendResult = defaultMQProducer.send(msg);
            log.info("sendMsg = " + sendResult.toString());
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
            throw checkProducerException(msg.getTopic(), "", ex);
        }
    }

    /**
     * @desc 发送有序消息，通过取模方式选择发送队列
     * @param msg
     * @param order
     * @throws Exception
     */
    public SendResult sendOrderMsg(Message msg, Integer order)  {
        try {
            SendResult sendResult = defaultMQProducer.send(msg, new MessageQueueSelector() {
                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                    Integer id = (Integer) arg;
                    int index = id.intValue() % mqs.size();
                    return mqs.get(index);
                }
            }, order);
            log.info("sendOrderMsg = " + sendResult.toString());
            return sendResult;
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
            throw checkProducerException(msg.getTopic(), "", ex);
        }
    }

    /**
     * @desc 通过SelectMessageQueueByHash实现发送队列选择
     * @param msg
     * @param order
     * @return
     * @throws Exception
     */
    public SendResult sendOrderMsg(Message msg, String order) {
        try {
            SendResult sendResult = defaultMQProducer.send(msg, new SelectMessageQueueByHash(), order);
            log.info("sendOrderMsg by SelectMessageQueueByHash =" + sendResult.toString());
            return sendResult;
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
            throw checkProducerException(msg.getTopic(), "", ex);
        }
    }

    /**
     * 异步发送消息
     * @param msg
     * @param sendCallback
     */
    public void sendAsync(Message msg, SendCallback sendCallback) {
        try {
            defaultMQProducer.send(msg, sendCallback);
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
            throw checkProducerException(msg.getTopic(), "", ex);
        }
    }

    /**
     * 关闭Producer
     * @throws Exception
     */
    public void shutdown() throws Exception {
        defaultMQProducer.shutdown();
    }

    private MQException checkProducerException(String topic, String msgId, Throwable e) {
        if (e instanceof MQClientException) {
            //
            if (e.getCause() != null) {
                // 无法连接Broker
                if (e.getCause() instanceof RemotingConnectException) {
                    return new MQException(
                            String.format("Connect broker failed, Topic=%s, msgId=%s", topic, msgId),e);
                }
                // 发送消息超时
                else if (e.getCause() instanceof RemotingTimeoutException) {
                    return new MQException(String.format("Send message to broker timeout, %dms, Topic=%s, msgId=%s",
                            this.defaultMQProducer.getSendMsgTimeout(), topic, msgId),e);
                }
                // Broker返回异常
                else if (e.getCause() instanceof MQBrokerException) {
                    MQBrokerException excep = (MQBrokerException) e.getCause();
                    return new MQException(
                            String.format("Receive a broker exception, Topi=%s, msgId=%s, %s", topic, msgId, excep.getErrorMessage()),excep);
                }
            }
            // 纯客户端异常
            else {
                MQClientException excep = (MQClientException) e;
                if (-1 == excep.getResponseCode()) {
                    return new MQException(
                            String.format("Topic does not exist, Topic=%s, msgId=%s", topic, msgId),e);
                } else if (ResponseCode.MESSAGE_ILLEGAL == excep.getResponseCode()) {
                    return new MQException(String.format("ONS Client check message exception, Topic=%s, msgId=%s", topic, msgId),excep);
                }
            }
        }
        return new MQException("defaultMQProducer send exception", e);
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

    public String getInstanceName() {
        return instanceName;
    }

    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }
}
