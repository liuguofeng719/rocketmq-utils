package com.smzc.util.rocketMQUtil.test;

import com.smzc.util.rocketMQUtil.MQListener;

import org.apache.rocketmq.common.message.MessageExt;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 *
 * @author lgfcxx
 * @version 1.0
 * @desc
 * @createtime 2018/10/10 上午11:40
 * @see jdk 1.7
 **/
public class TestOutOrderListener implements MQListener {

    public String getTopic() {
        return "automatic_usertag_topic";
    }

    public String getTags() {
        return "automatic_usertag_topic_tag";
    }

    final Semaphore semaphore = new Semaphore(2);

    public void action(MessageExt msg) {
        try {
            semaphore.acquire();
            try {
                System.out.println(msg.toString());
                TimeUnit.MILLISECONDS.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            semaphore.release();
        }
    }
}
