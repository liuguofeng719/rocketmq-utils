package com.smzc.util.rocketMQUtil.test;

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
    public static void main(String[] args) throws Exception{

        MQProducerImpl mqProducer = new MQProducerImpl();
        mqProducer.setProducerGroup("2323");
        mqProducer.setNamesrvAddr("10.211.55.4:9876");
        mqProducer.start();

        MQProducerImpl mqProducer2 = new MQProducerImpl();
        mqProducer2.setProducerGroup("eee");
        mqProducer2.setNamesrvAddr("10.211.55.5:9876");
        mqProducer2.start();

        System.out.println(mqProducer==mqProducer2);

//        mqProducer.setNamesrvAddr("10.28.18.144:9876;10.28.18.145:9876");
//        mqProducer.setNamesrvAddr("10.211.55.4:9876");
//        mqProducer.setProducerGroup("user-portrait-provider");

//        try {
//            for (int i = 0; i < 2; i++) {
//                Message msg = new Message();
//                msg.setTopic("automatic_usertag_topic");
//                msg.setTags("automatic_usertag_topic_tag");
//                msg.setKeys("keyxx");
//                msg.setBody(("哟哟check now xx ").getBytes(RemotingHelper.DEFAULT_CHARSET));
//                mqProducer.sendOrderMsg(msg,"123");
//            }
////            for (int i = 0; i <2 ; i++) {
////                Message msg1 = new Message();
////                msg1.setTopic("test1");
////                msg1.setTags("TagB1");
////                msg1.setKeys("keyxx");
////                msg1.setBody(("哟哟check now xx ").getBytes(RemotingHelper.DEFAULT_CHARSET));
////                mqProducer.sendMsg(msg1);
////            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        send1(mqProducer);
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
