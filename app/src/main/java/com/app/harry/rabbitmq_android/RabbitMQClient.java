package com.app.harry.rabbitmq_android;

import android.support.annotation.NonNull;
import android.text.TextUtils;
import android.util.Log;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
    public static String SERVICE_HOST_IP;
    public static int SERVICE_PORT;
    public static String SERVICE_USERNAME;
    public static String SERVICE_PASSWORD;
    public static String EXCHANGE_NAME;
    public static String EXCHANGE_TYPE;

    private final String FLAG_SEND = "send";
    private final String FLAG_RECEIVE = "receive";
    private List<String> exchangeTypeList = new ArrayList<>(4);

    private static RabbitMQClient singleton;
    private final ConnectionFactory factory;
    private Connection connection;
    private Map<String, Channel> channelMap = new HashMap<>();

    private RabbitMQClient() {
        factory = new ConnectionFactory();

        factory.setHost(SERVICE_HOST_IP);
        factory.setPort(SERVICE_PORT);
        factory.setUsername(SERVICE_USERNAME);
        factory.setPassword(SERVICE_PASSWORD);

        factory.setConnectionTimeout(10000);         //连接时间设置为10秒
        factory.setAutomaticRecoveryEnabled(true);   //恢复连接，通道
        factory.setTopologyRecoveryEnabled(true);    //恢复通道中 转换器，队列，绑定关系等
        factory.setNetworkRecoveryInterval(5000);    //恢复连接间隔，默认5秒

        exchangeTypeList.add("fanout"); //不用匹配路由，发送给所有绑定转换器的队列
        exchangeTypeList.add("direct"); //匹配路由一致，才发送给绑定转换器队列
        exchangeTypeList.add("topic");  // 通配符* 和 # 匹配路由一致，才发送给绑定转换器队列
        exchangeTypeList.add("headers");
    }

    public static RabbitMQClient getInstance() {
        if (singleton == null) {
            synchronized (RabbitMQClient.class) {
                if (singleton == null) {
                    if (TextUtils.isEmpty(SERVICE_HOST_IP))
                        throw new NullPointerException("请先初始化连接服务端配置条件");
                    singleton = new RabbitMQClient();
                }
            }
        }
        return singleton;
    }

    /**
     * first
     * 不定义转换器（默认匿名转换器），发送消息到指定队列中
     */
    public void sendQueueMessage(String message, String queueName) throws IOException, TimeoutException, AlreadyClosedException {
        if (connection == null || !connection.isOpen()) {
            connection = factory.newConnection();
        }

        if (!channelMap.containsKey(FLAG_SEND + queueName)) {
            Channel channel = connection.createChannel();
            channel.queueDeclare(queueName, false, false, false, null);
            channelMap.put(FLAG_SEND + queueName, channel);
        }

        channelMap.get(FLAG_SEND + queueName).basicPublish("", queueName, null, message.getBytes());
        Log.d(TAG, "队列-" + queueName + "-发送消息=====" + message);
    }

    /**
     * second
     * 自定义默认转换器名称和类型，不指定队列，消息根据routingkey路由选择队列（fanout类型转换器例外，发送到绑定该转换器的所有队列中）
     */
    public void sendRoutingKeyMessage(String message, String routingkey) throws IOException, TimeoutException, AlreadyClosedException {
        if (TextUtils.isEmpty(EXCHANGE_NAME) && !exchangeTypeList.contains(EXCHANGE_TYPE)) {
            throw new NullPointerException("请先设置默认转换器名称和正确类型，否则请调用指定转换器名称和类型的方法");
        }
        sendQueueRoutingKeyMessage(message, routingkey, EXCHANGE_NAME, EXCHANGE_TYPE);
    }

    /**
     * third
     * 自定义转换器名称和类型，指定队列，消息根据routingkey路由选择队列（fanout类型转换器例外，发送到绑定该转换器的所有队列中）
     */
    public void sendQueueRoutingKeyMessage(String message, String routingKey, @NonNull String exchangeName,
                                           String exchangeType) throws IOException, TimeoutException, AlreadyClosedException {
        if (connection == null || !connection.isOpen()) {
            connection = factory.newConnection();
        }

        if (!channelMap.containsKey(FLAG_SEND + routingKey + exchangeType)) {
            Channel channel = connection.createChannel();
            if (!TextUtils.isEmpty(exchangeName) && exchangeTypeList.contains(exchangeType)) {
                channel.exchangeDeclare(exchangeName, exchangeType);
            } else {
                channel.queueDeclare(routingKey, false, false, false, null);
            }
            channelMap.put(FLAG_SEND + routingKey + exchangeType, channel);
        }

        AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()    //设置消息属性
                .contentType("text/plain")
                .deliveryMode(2)
                .priority(1)
                .build();

        channelMap.get(FLAG_SEND + routingKey + exchangeType).basicPublish(exchangeName, routingKey, props, message.getBytes());
        Log.d(TAG, "路由-" + routingKey + "-发送消息=====" + message);
    }

    /**
     * 对应发送：first
     * 不定义转换器（默认匿名装换器），指定队列，获取实时和缓存在队列中的消息
     */
    public void receiveQueueMessage(final String queueName, final ResponseListener listener)
            throws IOException, TimeoutException, AlreadyClosedException {
        if (TextUtils.isEmpty(queueName)) {
            throw new NullPointerException("队列名字不能为空");
        }
        receiveQueueRoutingKeyMessage(queueName, "", listener);
    }

    /**
     * 对应发送： second,third
     * 自定义默认转换器名称和类型，不指定队列，监听符合routingKey的消息，只能获取实时消息
     */
    public void receiveRoutingKeyMessage(final String routingKey, final ResponseListener listener)
            throws IOException, TimeoutException, AlreadyClosedException {
        if (TextUtils.isEmpty(routingKey)) {
            throw new NullPointerException("转换器路由不能设置为空");
        }
        receiveQueueRoutingKeyMessage("", routingKey, listener);
    }

    /**
     * 对应发送：second,third
     * 自定义默认非持久化转换器名称和类型，自定义队列，监听符合routingKey的消息，获取实时和缓存在队列中的消息
     */
    public void receiveQueueRoutingKeyMessage(final String queueName, final String routingKey, final ResponseListener listener)
            throws IOException, TimeoutException, AlreadyClosedException {
        if (TextUtils.isEmpty(EXCHANGE_NAME) && !exchangeTypeList.contains(EXCHANGE_TYPE)) {
            throw new NullPointerException("请先设置默认转换器名称和正确类型，否则请调用指定转换器名称和类型的方法");
        }
        receiveQueueRoutingKeyMessage(queueName, routingKey, EXCHANGE_NAME, EXCHANGE_TYPE, listener);
    }

    /**
     * 对应发送：second,third
     * 自定义非持久化转换器名称和类型，自定义队列，监听符合routingKey的消息，获取实时和缓存在队列中的消息
     */
    public void receiveQueueRoutingKeyMessage(String queueName, final String routingKey, String exchangeName, String exchangeType, final ResponseListener listener)
            throws IOException, TimeoutException, AlreadyClosedException {
        if (!TextUtils.isEmpty(routingKey)) {
            if (TextUtils.isEmpty(exchangeName) || exchangeTypeList.contains(exchangeType)) {
                throw new NullPointerException("转换器名称不能为空并且转换器类型必须正确");
            }
        }

        if (connection == null || !connection.isOpen()) {
            connection = factory.newConnection();
        }

        if (!channelMap.containsKey(FLAG_RECEIVE + routingKey + queueName)) {
            final Channel channel = connection.createChannel();
            //自定义队列名称，还是匿名队列
            if (TextUtils.isEmpty(queueName)) {
                queueName = channel.queueDeclare().getQueue();
            } else {
                channel.queueDeclare(queueName, false, false, false, null);
            }
            //绑定转换器，使用路由筛选消息
            if (!TextUtils.isEmpty(routingKey)) {
                channel.exchangeDeclare(exchangeName, exchangeType);
                channel.queueBind(queueName, exchangeName, routingKey);  //设置绑定
            }
            //监听队列
            channel.basicConsume(queueName, false, new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope,
                                           AMQP.BasicProperties properties, byte[] body)
                        throws IOException {
                    String message = new String(body, "UTF-8");
                    if (listener != null) {
                        listener.receive(message);
                    }
                    Log.d(TAG, "路由-" + routingKey + "-接受消息---->" + message);
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
        channelMap.clear();
        if (connection != null && connection.isOpen()) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public interface ResponseListener {
        void receive(String message);
    }
}
