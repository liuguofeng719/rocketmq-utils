package com.smzc.util.rocketMQUtil.test;

import com.smzc.util.rocketMQUtil.MQListener;
import com.smzc.util.rocketMQUtil.impl.MQConsumerImpl;

import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 *
 * @author lgfcxx
 * @version 1.0
 * @desc
 * @createtime 2018/10/10 上午11:20
 * @see jdk 1.7
 **/
public class ConsumerTest {

    public static void main(String[] args) throws Exception {
        List<MQListener> listeners =new ArrayList<MQListener>();
        listeners.add(new TestOutOrderListener());
        MQConsumerImpl mqConsumer = new MQConsumerImpl();
        mqConsumer.setNamesrvAddr("10.28.17.121:9876;10.28.17.131:9876");
        mqConsumer.setConsumerGroup("user-portrait-provider");
        mqConsumer.setMqListeners(listeners);
        mqConsumer.start();
    }

}
