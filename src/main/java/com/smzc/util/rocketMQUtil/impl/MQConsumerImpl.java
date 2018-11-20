package com.smzc.util.rocketMQUtil.impl;

import com.smzc.util.rocketMQUtil.MQConsumer;
import com.smzc.util.rocketMQUtil.MQListener;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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

    private DefaultMQPushConsumer defaultMQPushConsumer;
    /**
     * 标识服务是否启动
     */
    private AtomicBoolean started = new AtomicBoolean(false);

    private ConcurrentHashMap<String/*topic*/, Map<String/*tag*/, List<MQListener>>> listenerMap =
            new ConcurrentHashMap<String, Map<String, List<MQListener>>>();
    /**
     * namesrv服务
     */
    private String namesrvAddr;
    /**
     * 分组名字
     */
    private String consumerGroup;
    /**
     * 消息消费执行的回调事件
     */
    private List<MQListener> mqListeners;
    /**
     * InstanceName
     */
    private String instanceName;
    /**
     * 是否为有序消息，默认为false
     */
    private boolean isOrder = false;
    /**
     * Minimum defaultMQPushConsumer thread number
     */
    private int consumeThreadMin = 20;
    /**
     * Max defaultMQPushConsumer thread number
     */
    private int consumeThreadMax = 64;
    /**
     * 消息模型
     */
    private MessageModel messageModel = MessageModel.CLUSTERING;

    private ConsumeFromWhere consumeFromWhere = ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET;

    private Lock lock = new ReentrantLock();

    public void start() throws Exception {

        try {
            lock.lock();
            if (defaultMQPushConsumer == null) {
                defaultMQPushConsumer = new DefaultMQPushConsumer(consumerGroup);
            }

            if (StringUtils.isBlank(namesrvAddr)) {
                throw new Exception("MQ服务器地址为空!");
            }

            defaultMQPushConsumer.setNamesrvAddr(namesrvAddr);

            if (StringUtils.isNotBlank(instanceName)) {
                defaultMQPushConsumer.setInstanceName(instanceName);
            }
            defaultMQPushConsumer.setConsumeThreadMin(getConsumeThreadMin());
            defaultMQPushConsumer.setConsumeThreadMax(getConsumeThreadMax());
            defaultMQPushConsumer.setMessageModel(messageModel);

            if (this.mqListeners != null && this.mqListeners.size() > 0) {
                try {
                    if (logger.isInfoEnabled()) {
                        logger.info("开始启动MQ客户端!");
                    }

                    this.initListenerMap();
                    this.subscribe();
                    this.registerListener();
                    if (this.started.compareAndSet(false, true)) {
                        defaultMQPushConsumer.start();
                    }
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
        } finally {
            lock.unlock();
        }
    }

    public void shutdown() throws Exception {
        if (this.started.compareAndSet(true, false)) {
            if (logger.isInfoEnabled()) {
                logger.info("开始停止MQ客户端!");
            }
            defaultMQPushConsumer.shutdown();
            defaultMQPushConsumer = null;
            if (logger.isInfoEnabled()) {
                logger.info("MQ Client 停止成功!");
            }
        }
    }

    private void subscribe() throws Exception {
        String topic;
        String tags;
        for (Iterator<MQListener> iterator = this.mqListeners.iterator(); iterator.hasNext(); defaultMQPushConsumer.subscribe(topic, tags)) {
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
        defaultMQPushConsumer.setConsumeFromWhere(getConsumeFromWhere());
        if (isOrder) {
            defaultMQPushConsumer.registerMessageListener(new MessageListenerOrderlyImpl());
        } else {
            defaultMQPushConsumer.registerMessageListener(new MessageListenerImpl());
        }
    }

    class MessageListenerOrderlyImpl implements MessageListenerOrderly {
        public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
            boolean orderSuccess = false;
            Iterator<MessageExt> it = msgs.iterator();

            while (it.hasNext()) {
                MessageExt msg = it.next();
                List<MQListener> listeners = fetchListener(msg.getTopic(), msg.getTags());
                Iterator<MQListener> iterator = listeners.iterator();

                while (iterator.hasNext()) {
                    MQListener mql = iterator.next();
                    try {
                        mql.action(msg);
                        orderSuccess = true;
                    } catch (Exception e) {
                        orderSuccess = false;
                        logger.warn("处理有序MQ消息失败!", e);
                    }
                }
            }

            if (orderSuccess) {
                return ConsumeOrderlyStatus.SUCCESS;
            } else {
                return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
            }
        }
    }

    class MessageListenerImpl implements MessageListenerConcurrently {
        public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
            boolean success = false;
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
                        logger.warn("处理无序MQ消息失败!", e);
                    }
                }
            }

            if (success) {
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            } else {
                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
        }
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

    public DefaultMQPushConsumer getDefaultMQPushConsumer() {
        return defaultMQPushConsumer;
    }

    public void setDefaultMQPushConsumer(DefaultMQPushConsumer defaultMQPushConsumer) {
        defaultMQPushConsumer = defaultMQPushConsumer;
    }

    public int getConsumeThreadMin() {
        return consumeThreadMin;
    }

    public void setConsumeThreadMin(int consumeThreadMin) {
        this.consumeThreadMin = consumeThreadMin;
    }

    public int getConsumeThreadMax() {
        return consumeThreadMax;
    }

    public void setConsumeThreadMax(int consumeThreadMax) {
        this.consumeThreadMax = consumeThreadMax;
    }

    public List<MQListener> getMqListeners() {
        return mqListeners;
    }

    public void setMqListeners(List<MQListener> mqListeners) {
        this.mqListeners = mqListeners;
    }

    public boolean isOrder() {
        return isOrder;
    }

    public void setOrder(boolean order) {
        isOrder = order;
    }

    public String getInstanceName() {
        return instanceName;
    }

    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }

    public ConsumeFromWhere getConsumeFromWhere() {
        return consumeFromWhere;
    }

    public void setConsumeFromWhere(ConsumeFromWhere consumeFromWhere) {
        this.consumeFromWhere = consumeFromWhere;
    }

    public MessageModel getMessageModel() {
        return messageModel;
    }

    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = messageModel;
    }
}
