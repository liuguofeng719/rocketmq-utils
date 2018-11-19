package com.smzc.util.rocketMQUtil.test;

import com.smzc.util.rocketMQUtil.MQListener;

import org.apache.rocketmq.common.message.MessageExt;

/**
 * Created with IntelliJ IDEA.
 *
 * @author lgf
 * @version 1.0
 * @desc
 * @createtime 2018/11/17 2:51 PM
 * @see jdk 1.7
 **/
public class MyLisenterCOrder implements MQListener {

    public String getTopic() {
        return "MyC";
    }

    public String getTags() {
        return "MyC";
    }

    public void action(MessageExt msg) {
        final String topic = msg.getTopic();
        final byte[] msgBody = msg.getBody();
        String content = new String(msgBody);
        System.out.println("有序消息 MyLisenterCOrder=== topic = " + topic + " msgs = " + content);
    }
}
