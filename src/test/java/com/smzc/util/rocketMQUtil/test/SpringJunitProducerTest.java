package com.smzc.util.rocketMQUtil.test;

import com.smzc.util.rocketMQUtil.MQProducer;

import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.annotation.Resource;

/**
 * Created with IntelliJ IDEA.
 *
 * @author lgf
 * @version 1.0
 * @desc
 * @createtime 2018/11/17 3:08 PM
 * @see jdk 1.7
 **/
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "classpath:spring.xml")
public class SpringJunitProducerTest {

    @Resource
    private MQProducer producerA;

    @Resource
    private MQProducer producerB;

    @Test
    public void send() throws Exception {
        for (int i = 0; i < 1; i++) {
            Message msg = new Message();
            msg.setTopic("MyA");
            msg.setTags("MyA");
            msg.setKeys("MyA");
            msg.setBody(("哟哟MyA ").getBytes(RemotingHelper.DEFAULT_CHARSET));
            producerA.sendMsg(msg);
        }

        for (int i = 0; i < 2; i++) {
            Message msg = new Message();
            msg.setTopic("MyA");
            msg.setTags("MyA");
            msg.setKeys("MyA");
            msg.setBody(("哟哟check now xx ").getBytes(RemotingHelper.DEFAULT_CHARSET));
            producerB.sendMsg(msg);
        }

        String[] body = {"创建订单", "支付订单", "已发货"};
        for (int i = 0; i < body.length; i++) {
            Message msg3 = new Message();
            msg3.setTopic("MyC");
            msg3.setTags("MyC");
            msg3.setKeys("MyC");
            msg3.setBody(body[i].getBytes(RemotingHelper.DEFAULT_CHARSET));
            producerB.sendOrderMsg(msg3, "20181117000002");
        }

        while (Thread.currentThread().isAlive()) {

        }
    }
}
