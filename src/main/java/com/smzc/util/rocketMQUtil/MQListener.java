package com.smzc.util.rocketMQUtil;


import org.apache.rocketmq.common.message.MessageExt;

/**
 * Created with IntelliJ IDEA.
 *
 * @author lgfcxx
 * @version 1.0
 * @desc
 * @createtime 2017/8/20 下午3:10
 * @see JDK 1.7
 **/
public interface MQListener {

    String getTopic();

    String getTags();

    void action(MessageExt msg);
}
