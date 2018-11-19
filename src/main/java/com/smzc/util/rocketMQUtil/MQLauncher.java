package com.smzc.util.rocketMQUtil;

import com.smzc.util.rocketMQUtil.impl.MQConsumerImpl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.ContextStoppedEvent;

/**
 * Created with IntelliJ IDEA.
 *
 * @author lgfcxx
 * @version 1.0
 * @desc
 * @createtime 2017/8/20 下午4:40
 * @see JDK 1.7
 **/
public class MQLauncher implements ApplicationListener {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private MQConsumer mqConsumer;

    public void onApplicationEvent(ApplicationEvent event) {
        if (event instanceof ContextRefreshedEvent) {
            this.start((ContextRefreshedEvent) event);
        } else {
            if (event instanceof ContextStoppedEvent) {
                this.stop(event);
            }

            if (event instanceof ContextClosedEvent) {
                this.stop(event);
            }
        }
    }

    public void start(ContextRefreshedEvent event) {
        try {
            if (this.mqConsumer == null) {
                this.mqConsumer = new MQConsumerImpl();;
                event.getApplicationContext().getAutowireCapableBeanFactory().autowireBean(this.mqConsumer);
            }
            this.mqConsumer.start();
        } catch (Exception ex) {
            this.log.error(ex.getMessage(), ex);
        }

    }

    public void stop(ApplicationEvent event) {
        try {
            if (this.mqConsumer != null) {
                this.mqConsumer.shutdown();
            }
        } catch (Exception ex) {
            this.log.error(ex.getMessage(), ex);
        }
    }

    public MQConsumer getMqConsumer() {
        return mqConsumer;
    }

    public void setMqConsumer(MQConsumer mqConsumer) {
        this.mqConsumer = mqConsumer;
    }
}
