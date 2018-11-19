package com.smzc.util.rocketMQUtil;

/**
 * Created with IntelliJ IDEA.
 *
 * @author lgf
 * @version 1.0
 * @desc
 * @createtime 2018/11/19 5:41 PM
 * @see jdk 1.7
 **/
public class MQException extends RuntimeException {

    public MQException() {
        super();
    }

    public MQException(String message) {
        super(message);
    }

    public MQException(String message, Throwable cause) {
        super(message, cause);
    }

    public MQException(Throwable cause) {
        super(cause);
    }
}
