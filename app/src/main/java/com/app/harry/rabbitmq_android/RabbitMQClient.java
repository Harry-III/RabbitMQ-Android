package com.app.harry.rabbitmq_android;

import android.support.annotation.NonNull;
import android.text.TextUtils;
import android.util.Log;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * Created by Harry on 2017/8/17
 */
public class RabbitMQClient {
    private final String TAG = "RabbitMQ";
    /**
     * 需要自己设置，这里给出的只是样本
     */
    public static final String SERVICE_HOST_IP = "127.0.0.1";
    public static final int SERVICE_PORT = 5672;
    public static final String SERVICE_USERNAME = "admin";
    public static final String SERVICE_PASSWORD = "admin";
    private final String EXCHANGE_NAME = "exchangeName";
    private final String EXCHANGE_TYPE = "topic";   //四种类型，fanout, direct , topic ,headers

    private final ConnectionFactory factory;
    private Connection connection;
    private Map<String, Channel> channelMap = new HashMap<>();
    //默认配置，避免channelMap中的key一样
    private final String FLAG_SEND = "send";
    private final String FLAG_RECEIVE = "receive";

    private static RabbitMQClient singleton;

    public RabbitMQClient() {
        /**
         * 创建RabbitMQ的工厂以及设置条件
         */

        factory = new ConnectionFactory();

        factory.setHost(SERVICE_HOST_IP);
        factory.setPort(SERVICE_PORT);
        factory.setUsername(SERVICE_USERNAME);
        factory.setPassword(SERVICE_PASSWORD);

        factory.setAutomaticRecoveryEnabled(true);   //恢复连接
        factory.setNetworkRecoveryInterval(5000);   //恢复连接间隔，默认5秒
    }

    public static RabbitMQClient getInstance() {
        if (singleton == null) {
            synchronized (RabbitMQClient.class) {
                if (singleton == null) {
                    singleton = new RabbitMQClient();
                }
            }
        }
        return singleton;
    }

    /**
     * first
     * 不定义装换器（默认匿名装换器），发送消息到指定队列中
     */
    public void sendQueueMessage(String message, String queueName) throws IOException, TimeoutException {
        if (connection == null || !connection.isOpen()) {
            connection = factory.newConnection();
        }
        if (!channelMap.containsKey(FLAG_SEND + queueName)) {
            Channel channel = connection.createChannel();
            channel.queueDeclare(queueName, false, false, false, null);
            channelMap.put(FLAG_SEND + queueName, channel);
        }

        channelMap.get(FLAG_SEND + queueName).basicPublish("", queueName, null, message.getBytes());
        Log.d(TAG, queueName + "发送消息=====" + message);
    }

    /**
     * second
     * 自定义默认装换器名称和类型，不指定队列，消息根据routingkey路由选择队列（fanout类型装换器例外，发送到绑定该装换器的所有队列中）
     */
    public void sendRoutingKeyMessage(String message, String routingkey) throws IOException, TimeoutException {
        if (connection == null || !connection.isOpen()) {
            connection = factory.newConnection();
        }

        if (!channelMap.containsKey(FLAG_SEND + routingkey)) {
            Channel channel = connection.createChannel();
            channel.exchangeDeclare(EXCHANGE_NAME, EXCHANGE_TYPE);
            channelMap.put(FLAG_SEND + routingkey, channel);
        }

        channelMap.get(FLAG_SEND + routingkey).basicPublish(EXCHANGE_NAME, routingkey, null, message.getBytes());
        Log.d(TAG, routingkey + "路由发送消息" + message);
    }

    /**
     * third
     * 自定义装换器名称和类型，指定队列，消息根据routingkey路由选择队列（fanout类型装换器例外，发送到绑定该装换器的所有队列中）
     */
    public void sendQueueRoutingKeyMessage(String message, String routingKey, @NonNull String exchangeName,
                                           String exchangeType) throws IOException, TimeoutException {
        if (connection == null || !connection.isOpen()) {
            connection = factory.newConnection();
        }

        if (!channelMap.containsKey(FLAG_SEND + routingKey + exchangeType)) {
            Channel channel = connection.createChannel();
            if (!TextUtils.isEmpty(exchangeName)) {
                channel.exchangeDeclare(exchangeName, exchangeType);
            } else {
                channel.queueDeclare(routingKey, false, false, false, null);
            }
        }

        channelMap.get(FLAG_SEND + routingKey + exchangeType).basicPublish(exchangeName, routingKey, null, message.getBytes());
        Log.d(TAG, routingKey + "路由发送消息" + message);
    }

    /**
     * 对应发送：first
     * 不定义装换器（默认匿名装换器），指定队列，获取实时和缓存在队列中的消息
     */
    public void receiveQueueMessage(final String queueName, final ReceiveMessageListener listener)
            throws IOException, TimeoutException {
        if (connection == null || !connection.isOpen()) {
            connection = factory.newConnection();
        }

        if (!channelMap.containsKey(FLAG_RECEIVE + queueName)) {
            final Channel channel = connection.createChannel();
            channel.queueDeclare(queueName, false, false, false, null);

            channel.basicConsume(queueName, false, new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope,
                                           AMQP.BasicProperties properties, byte[] body)
                        throws IOException {
                    String message = new String(body, "UTF-8");
                    if (listener != null) {
                        listener.receive(message);
                    }
                    Log.d(TAG, queueName + "接受消息->" + message);
                    channel.basicAck(envelope.getDeliveryTag(), false);  //消息应答
                }
            });
            channelMap.put(FLAG_RECEIVE + queueName, channel);
        }
    }

    /**
     * 对应发送： second,third
     * 自定义默认装换器名称和类型，不指定队列，监听符合routingKey的消息，只能获取实时消息
     */
    public void receiveRoutingKeyMessage(final String routingKey, final ReceiveMessageListener listener)
            throws IOException, TimeoutException {
        if (connection == null || !connection.isOpen()) {
            connection = factory.newConnection();
        }

        if (!channelMap.containsKey(FLAG_RECEIVE + routingKey)) {
            final Channel channel = connection.createChannel();

            channel.exchangeDeclare(EXCHANGE_NAME, EXCHANGE_TYPE);
            String queueName = channel.queueDeclare().getQueue();
            channel.queueBind(queueName, EXCHANGE_NAME, routingKey);

            channel.basicConsume(queueName, false, new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope,
                                           AMQP.BasicProperties properties, byte[] body)
                        throws IOException {
                    String message = new String(body, "UTF-8");
                    if (listener != null) {
                        listener.receive(message);
                    }
                    Log.d(TAG, routingKey + "接受消息->" + message);
                    channel.basicAck(envelope.getDeliveryTag(), false);  //消息应答
                }
            });
            channelMap.put(FLAG_RECEIVE + routingKey, channel);
        }
    }

    /**
     * 对应发送：second,third
     * 自定义默认装换器名称和类型，自定义队列，监听符合routingKey的消息，获取实时和缓存在队列中的消息
     */
    public void receiveQueueRoutingKeyMessage(final String queueName, final String routingKey,
                                              final ReceiveMessageListener listener) throws IOException, TimeoutException {
        if (connection == null || !connection.isOpen()) {
            connection = factory.newConnection();
        }

        if (!channelMap.containsKey(FLAG_RECEIVE + routingKey + queueName)) {
            final Channel channel = connection.createChannel();

            channel.exchangeDeclare(EXCHANGE_NAME, EXCHANGE_TYPE);
            channel.queueDeclare(queueName, false, false, false, null);
            channel.queueBind(queueName, EXCHANGE_NAME, routingKey);  //设置绑定

            channel.basicConsume(queueName, false, new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope,
                                           AMQP.BasicProperties properties, byte[] body)
                        throws IOException {
                    String message = new String(body, "UTF-8");
                    if (listener != null) {
                        listener.receive(message);
                    }
                    Log.d(TAG, routingKey + queueName + "接受消息->" + message);
                    channel.basicAck(envelope.getDeliveryTag(), false);  //消息应答
                }
            });
            channelMap.put(FLAG_RECEIVE + routingKey + queueName, channel);
        }

    }

    /**
     * 关闭所有资源
     */
    public void close() {
        for (Channel next : channelMap.values()) {
            if (next != null && next.isOpen()) {
                try {
                    next.close();
                } catch (IOException | TimeoutException e) {
                    e.printStackTrace();
                }
            }
        }
        if (connection != null && connection.isOpen()) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public interface ReceiveMessageListener {
        void receive(String message);
    }
}
