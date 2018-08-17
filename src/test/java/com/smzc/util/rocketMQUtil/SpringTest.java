package com.smzc.util.rocketMQUtil;

import com.smzc.util.rocketMQUtil.impl.MQProducerImpl;

import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import javax.annotation.Resource;

/**
 * Created with IntelliJ IDEA.
 *
 * @author lgfcxx
 * @version 1.0
 * @desc
 * @createtime 2017/8/21 下午2:05
 * @see JDK 1.7
 **/
public class SpringTest {
//    @Resource MQProducer mqProducer;
    public static void main(String[] args) {
        MQProducerImpl mqProducer = new MQProducerImpl();
        mqProducer.setNamesrvAddr("10.211.55.4:9876;10.211.55.5:9876");
        mqProducer.setProducerGroup("test");

        try {
            Message msg = new Message();
            msg.setTopic("User");
            msg.setTags("TagB");
            msg.setKeys("keyxx");
            msg.setBody(("哟哟check now xx ").getBytes(RemotingHelper.DEFAULT_CHARSET));
            mqProducer.sendMsg(msg);
//            send1(mqProducer);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void send1(MQProducerImpl mqProducer) throws Exception {
        String[] tags = new String[]{"TagA", "TagB", "TagC", "TagD", "TagE"};
        for (int i = 0; i < tags.length; i++) {
            Message msg = new Message();
            msg.setTopic("UserTest");
            msg.setTags(tags[i % tags.length]);
            msg.setKeys("keyxx" + i);
            msg.setBody(("哟哟check now xx " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            mqProducer.sendMsg(msg);
        }
    }
}
