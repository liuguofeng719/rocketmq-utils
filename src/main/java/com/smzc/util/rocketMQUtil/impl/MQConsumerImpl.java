package com.smzc.util.rocketMQUtil.impl;

import com.smzc.util.rocketMQUtil.MQConsumer;
import com.smzc.util.rocketMQUtil.MQListener;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 *
 * @author lgfcxx
 * @version 1.0
 * @desc
 * @createtime 2017/8/20 下午3:08
 * @see JDK 1.7
 **/
public class MQConsumerImpl implements MQConsumer {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private DefaultMQPushConsumer consumer;
    private Boolean started = false;
    private Map<String, Map<String, List<MQListener>>> listenerMap = new HashMap<String, Map<String, List<MQListener>>>();
    private String namesrvAddr;
    private String consumerGroup;
    private boolean broadcasting = false;
    private List<MQListener> mqListeners;

    public boolean isBroadcasting() {
        return broadcasting;
    }

    public void setBroadcasting(boolean broadcasting) {
        this.broadcasting = broadcasting;
    }

    public List<MQListener> getMqListeners() {
        return mqListeners;
    }

    public void setMqListeners(List<MQListener> mqListeners) {
        this.mqListeners = mqListeners;
    }

    public DefaultMQPushConsumer consumer() throws Exception {
        if (consumer == null) {
            if (StringUtils.isBlank(consumerGroup)) {
                consumer = new DefaultMQPushConsumer();
            } else {
                consumer = new DefaultMQPushConsumer(consumerGroup);
            }

            if (StringUtils.isBlank(namesrvAddr)) {
                throw new Exception("MQ服务器地址为空!");
            }

            consumer.setNamesrvAddr(namesrvAddr);
            if (broadcasting) {
                consumer.setMessageModel(MessageModel.BROADCASTING);
            }
        }

        return consumer;
    }

    public void startListener() throws Exception {
        if (!this.started.booleanValue()) {
            this.started = true;
            if (this.mqListeners != null && this.mqListeners.size() > 0) {
                try {
                    if (logger.isInfoEnabled()) {
                        logger.info("开始启动MQ客户端!");
                    }

                    this.initListenerMap();
                    this.subscribe();
                    this.registerListener();
                    consumer().start();
                    if (logger.isInfoEnabled()) {
                        logger.info("MQ Client 启动成功!");
                    }
                } catch (MQClientException ex) {
                    logger.error("MQ Client启动失败!" + ex.getMessage(), ex);
                }

            } else {
                if (logger.isInfoEnabled()) {
                    logger.info("没有监听器, 不启动MQ消息监听!");
                }

            }
        }
    }

    public void stopListener() throws Exception {
        if (logger.isInfoEnabled()) {
            logger.info("开始停止MQ客户端!");
        }

        consumer().shutdown();
        if (logger.isInfoEnabled()) {
            logger.info("MQ Client 停止成功!");
        }
        consumer = null;
    }

    private void subscribe() throws Exception {
        String topic;
        String tags;
        for (Iterator<MQListener> iterator = this.mqListeners.iterator(); iterator.hasNext(); consumer().subscribe(topic, tags)) {
            MQListener listener = iterator.next();
            topic = listener.getTopic();
            tags = listener.getTags();
            if (logger.isInfoEnabled()) {
                logger.info("订阅: topic->" + topic + "   tags->" + tags);
            }
        }

    }

    private void initListenerMap() {
        Iterator<MQListener> listenerIterator = this.mqListeners.iterator();

        while (listenerIterator.hasNext()) {
            MQListener listener = listenerIterator.next();
            Map<String, List<MQListener>> topic = this.listenerMap.get(listener.getTopic());
            if (topic == null) {
                topic = new HashMap();
                this.listenerMap.put(listener.getTopic(), topic);
            }

            List<String> selfTag = this.splitMsg(listener.getTags(), "\\|\\|");
            Iterator<String> iterator = selfTag.iterator();

            while (iterator.hasNext()) {
                String tag = iterator.next();
                List<MQListener> items = topic.get(tag);
                if (items == null) {
                    items = new ArrayList();
                    topic.put(tag, items);
                }

                if (!items.contains(listener)) {
                    items.add(listener);
                }
            }
        }
    }

    private List<MQListener> fetchListener(String topic, String tags) {
        ArrayList list = new ArrayList();
        if (!this.listenerMap.containsKey(topic)) {
            return list;
        } else {
            Map tagMap = this.listenerMap.get(topic);
            List selfTag = this.splitMsg(tags, "\\|\\|");
            Iterator it = selfTag.iterator();

            while (it.hasNext()) {
                String tag = (String) it.next();
                List listeners = (List) tagMap.get(tag);
                Iterator iterator = listeners.iterator();

                while (iterator.hasNext()) {
                    MQListener mql = (MQListener) iterator.next();
                    if (!list.contains(mql)) {
                        list.add(mql);
                    }
                }
            }
            return list;
        }
    }

    private void registerListener() throws Exception {
        consumer().setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer().registerMessageListener(getMessageListener());
    }

    private MessageListenerConcurrently getMessageListener() {
        return new MessageListenerConcurrently() {
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                Boolean success = false;
                Iterator it = msgs.iterator();

                while (it.hasNext()) {
                    MessageExt msg = (MessageExt) it.next();
                    List listeners = fetchListener(msg.getTopic(), msg.getTags());
                    Iterator iterator = listeners.iterator();

                    while (iterator.hasNext()) {
                        MQListener mql = (MQListener) iterator.next();
                        try {
                            mql.action(msg);
                            success = true;
                        } catch (Exception e) {
                            success = false;
                            logger.warn("处理MQ消息失败!", e);
                        }
                    }
                }

                if (success.booleanValue()) {
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                } else {
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
            }
        };
    }


    private Boolean equalsTag(List<String> msgTags, String tags) {
        if (StringUtils.isBlank(tags)) {
            return true;
        } else if (tags.equals("*")) {
            return true;
        } else {
            List selfTag = this.splitMsg(tags, "\\|\\|");
            Iterator<String> iterator = selfTag.iterator();

            String key;
            do {
                if (!iterator.hasNext()) {
                    return false;
                }
                key = iterator.next();
            } while (!msgTags.contains(key));

            return true;
        }
    }

    private List<String> splitMsg(String tags, String regex) {
        String[] arr = tags.split(regex);
        ArrayList list = new ArrayList();
        int len = arr.length;
        for (int i = 0; i < len; ++i) {
            String t = arr[i];
            list.add(t.trim());
        }
        return list;
    }

    public String getNamesrvAddr() {
        return this.namesrvAddr;
    }

    public void setNamesrvAddr(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public DefaultMQPushConsumer getConsumer() {
        return consumer;
    }

    public void setConsumer(DefaultMQPushConsumer consumer) {
        consumer = consumer;
    }
}
