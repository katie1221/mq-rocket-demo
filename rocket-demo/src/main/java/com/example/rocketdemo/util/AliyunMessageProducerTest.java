package com.example.rocketdemo.util;

import com.aliyun.mq.http.MQClient;
import com.aliyun.mq.http.MQProducer;
import com.aliyun.mq.http.model.TopicMessage;
import com.example.rocketdemo.config.MqConfigParams;
import lombok.extern.slf4j.Slf4j;
import java.util.Date;

/**
 * 生产 阿里云 RocketMQ 消息
 * @author qzz
 */
@Slf4j
public class AliyunMessageProducerTest {
    public static void main(String[] args) {
        MQClient mqClient = new MQClient(
                // 设置HTTP协议客户端接入点，进入消息队列RocketMQ版控制台实例详情页面的接入点区域查看。
                MqConfigParams.HTTP_ENDPOINT,
                // AccessKey ID，阿里云身份验证，在阿里云RAM控制台创建。
                MqConfigParams.ACCESS_KEY,
                // AccessKey Secret，阿里云身份验证，在阿里云RAM控制台创建。
                MqConfigParams.SECRET_KEY
        );

        // 消息所属的Topic，在消息队列RocketMQ版控制台创建。
        // 不同消息类型的Topic不能混用，例如普通消息的Topic只能用于收发普通消息，不能用于收发其他类型的消息。
        final String topic = MqConfigParams.TOPIC;
        // Topic所属的实例ID，在消息队列RocketMQ版控制台创建。
        // 若实例有命名空间，则实例ID必须传入；若实例无命名空间，则实例ID传入null空值或字符串空值。实例的命名空间可以在消息队列RocketMQ版控制台的实例详情页面查看。
        final String instanceId = MqConfigParams.INSTANCE_ID;

        // 获取Topic的生产者。
        MQProducer producer;
        if (instanceId != null && instanceId != "") {
            producer = mqClient.getProducer(instanceId, topic);
        } else {
            producer = mqClient.getProducer(topic);
        }

        try {
            // 循环发送2条消息。
            for (int i = 0; i < 2; i++) {
                TopicMessage pubMsg;        // 普通消息。
                pubMsg = new TopicMessage(
                        // 消息内容。
                        "hello mq 111!".getBytes(),
                        // 消息标签。
                        MqConfigParams.TAG
                );
                // 设置消息的自定义属性。
                pubMsg.getProperties().put("a", String.valueOf(i));
                // 设置消息的Key。
                pubMsg.setMessageKey("MessageKey");

                // 同步发送消息，只要不抛异常就是成功。
                TopicMessage pubResultMsg = producer.publishMessage(pubMsg);

                // 同步发送消息，只要不抛异常就是成功。
                System.out.println(new Date() + " Send mq message success. Topic is:" + topic + ", msgId is: " + pubResultMsg.getMessageId()
                        + ", bodyMD5 is: " + pubResultMsg.getMessageBodyMD5());
            }
        } catch (Throwable e) {
            // 消息发送失败，需要进行重试处理，可重新发送这条消息或持久化这条数据进行补偿处理。
            System.out.println(new Date() + " Send mq message failed. Topic is:" + topic);
            e.printStackTrace();
        }

        mqClient.close();
    }


}
