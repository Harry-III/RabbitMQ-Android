package com.app.harry.rabbitmq_android;

import android.os.SystemClock;
import android.support.annotation.NonNull;
import android.util.Log;

import com.rabbitmq.client.AlreadyClosedException;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

/**
 * Created by Harry on 2017/8/17
 */
public class RabbitMQUtil {
    private final String TAG = "RabbitMQ";
    private static RabbitMQUtil singleton;
    private RabbitMQClient rabbitMQ;
    private ExecutorService executor;

    public RabbitMQUtil() {
        rabbitMQ = RabbitMQClient.getInstance();
        executor = Executors.newSingleThreadExecutor();  //根据项目需要设置常用线程个数
    }

    public static RabbitMQUtil getInstance() {
        if (singleton == null) {
            synchronized (RabbitMQClient.class) {
                if (singleton == null) {
                    singleton = new RabbitMQUtil();
                }
            }
        }
        return singleton;
    }

    public void sendQueueMessage(final String message, final String queueName, final SendMessageListener listener) {
        executor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    rabbitMQ.sendQueueMessage(message, queueName);
                    if (listener != null) listener.sendMessage(true);
                } catch (IOException | TimeoutException | AlreadyClosedException e) {
                    e.printStackTrace();
                    if (listener != null) listener.sendMessage(false);
                }
            }
        });

    }

    public void sendRoutingKeyMessage(final String message, final String routingKey, final SendMessageListener listener) {
        executor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    rabbitMQ.sendRoutingKeyMessage(message, routingKey);
                    if (listener != null) listener.sendMessage(true);
                } catch (IOException | TimeoutException | AlreadyClosedException e) {
                    e.printStackTrace();
                    if (listener != null) listener.sendMessage(false);
                }
            }
        });
    }

    public void sendQueueRoutingKeyMessage(final String message, final String routingKey, @NonNull final String exchangeName,
                                           final String exchangeType, final SendMessageListener listener) {
        executor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    // FIXME: 2017/12/13 不明白为什么发送消息是在线程池中进行，然后下一步是在该线程中执行？怎么可以返回结果呢
                    rabbitMQ.sendQueueRoutingKeyMessage(message, routingKey, exchangeName, exchangeType);
                    if (listener != null) listener.sendMessage(true);
                } catch (IOException | TimeoutException | AlreadyClosedException e) {
                    e.printStackTrace();
                    if (listener != null) listener.sendMessage(false);
                }
            }
        });
    }

    public void receiveQueueMessage(final String queueName, final ReceiveMessageListener listener) {
        executor.execute(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        rabbitMQ.receiveQueueMessage(queueName, new RabbitMQClient.ReceiveMessageListener() {

                            @Override
                            public void receive(String message) {
                                if (listener != null) listener.receiveMessage(message);
                            }

                        });
                        break;
                    } catch (IOException | TimeoutException | AlreadyClosedException e) {
                        e.printStackTrace();
                        Log.d(TAG, "未连接到-" + queueName + "-----5秒后自动重连");
                        SystemClock.sleep(5000);
                    }
                }
            }
        });
    }

    public void receiveRoutingKeyMessage(final String routingKey, final ReceiveMessageListener listener) {
        executor.execute(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        rabbitMQ.receiveRoutingKeyMessage(routingKey, new RabbitMQClient.ReceiveMessageListener() {
                            @Override
                            public void receive(String message) {
                                if (listener != null) listener.receiveMessage(message);
                            }
                        });
                        break;
                    } catch (IOException | TimeoutException | AlreadyClosedException e) {
                        e.printStackTrace();
                        Log.d(TAG, "未连接到-" + routingKey + "------5秒后自动重连");
                        SystemClock.sleep(5000);  //等待五秒
                    }
                }
            }
        });
    }

    public void receiveQueueRoutingKeyMessage(final String queueName, final String routingKey, final ReceiveMessageListener listener) {
        executor.execute(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        rabbitMQ.receiveQueueRoutingKeyMessage(queueName, routingKey, new RabbitMQClient.ReceiveMessageListener() {
                            @Override
                            public void receive(String message) {
                                if (listener != null) listener.receiveMessage(message);
                            }

                        });
                        break;
                    } catch (IOException | TimeoutException | AlreadyClosedException e) {
                        e.printStackTrace();
                        Log.d(TAG, "未连接到-" + routingKey + "------5秒后自动重连");
                        SystemClock.sleep(5000);  //等待五秒
                    }
                }

            }
        });

    }

    /**
     * 在application中关闭或者在结束工作时关闭
     */
    public void close() {
        rabbitMQ.close();
        executor.shutdown();
        singleton = null;
        Log.d(TAG, "关闭RabbitMQ");
    }

    public interface ReceiveMessageListener {
        void receiveMessage(String message);
    }

    public interface SendMessageListener {
        void sendMessage(boolean isSuccess);
    }


}
