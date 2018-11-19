package com.smzc.util.rocketMQUtil.test;

import com.smzc.util.rocketMQUtil.MQListener;

import org.apache.rocketmq.common.message.MessageExt;

/**
 * Created with IntelliJ IDEA.
 *
 * @author lgfcxx
 * @version 1.0
 * @desc
 * @createtime 2018/10/10 上午11:20
 * @see jdk 1.7
 **/
public class TestOrderMQListener implements MQListener {

    final String TOPIC_TEST = "test";

    public String getTopic() {
        return TOPIC_TEST;
    }

    public String getTags() {
        return "TagB";
    }

    public void action(MessageExt msg) {

    }
}
