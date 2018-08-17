package com.smzc.util.rocketMQUtil;

/**
 * Created with IntelliJ IDEA.
 *
 * @author lgfcxx
 * @version 1.0
 * @desc
 * @createtime 2017/8/20 下午2:42
 * @see JDK 1.7
 **/
public interface MQConsumer {
    void startListener() throws Exception;

    void stopListener() throws Exception;
}
