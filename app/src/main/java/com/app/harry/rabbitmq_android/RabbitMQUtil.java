package com.app.harry.rabbitmq_android;

import android.os.SystemClock;
import android.support.annotation.NonNull;
import android.util.Log;

import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

/**
 * Created by Harry on 2017/8/17
 */
public class RabbitMQUtil {
    private static RabbitMQUtil singleton;
    private final String TAG = "RabbitMQ";
    private final RabbitMQClient rabbitMQ;
    private final Executor executor;
    private boolean isConnectedOne = true;   //连接成功一次就好
    private boolean isFirstTimeOne = true;   //监听只需要设置一次就好
    private boolean isConnectedTwo = true;   //连接成功一次就好
    private boolean isFirstTimeTwo = true;   //监听只需要设置一次就好

    public RabbitMQUtil() {
        rabbitMQ = RabbitMQClient.getInstance();
        executor = Executors.newFixedThreadPool(3);
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
                } catch (IOException | TimeoutException e) {
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
                } catch (IOException | TimeoutException e) {
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
                    rabbitMQ.sendQueueRoutingKeyMessage(message, routingKey, exchangeName, exchangeType);
                    if (listener != null) listener.sendMessage(true);
                } catch (IOException | TimeoutException e) {
                    e.printStackTrace();
                    if (listener != null) listener.sendMessage(false);
                }
            }
        });
    }

    public void receiveQueueMessage(final String queueName, final ReceiveMessageListener listener) {
        if (!isFirstTimeOne) {
            return;
        }
        isFirstTimeOne = false;

        executor.execute(new Runnable() {
            @Override
            public void run() {
                while (isConnectedOne) {
                    try {
                        rabbitMQ.receiveQueueMessage(queueName, new RabbitMQClient.ReceiveMessageListener() {

                            @Override
                            public void receive(String message) {
                                if (listener != null) listener.receiveMessage(message);
                            }

                        });
                        isConnectedOne = false;
                    } catch (IOException | TimeoutException e) {
                        isConnectedOne = true;
                        e.printStackTrace();
                        SystemClock.sleep(5000);
                        Log.d(TAG, "未连接到queueMessage");
                    }
                }
            }
        });
    }

    public void receiveRoutingKeyMessage(final String routingKey, final ReceiveMessageListener listener) {
        executor.execute(new Runnable() {
            @Override
            public void run() {
                while (!isConnectedOne) {
                    try {
                        rabbitMQ.receiveRoutingKeyMessage(routingKey, new RabbitMQClient.ReceiveMessageListener() {
                            @Override
                            public void receive(String message) {
                                if (listener != null) listener.receiveMessage(message);
                            }
                        });
                    } catch (IOException | TimeoutException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
    }

    public void receiveQueueRoutingKeyMessage(final String queueName, final String routingKey, final ReceiveMessageListener listener) {
        if (!isFirstTimeTwo) {
            return;
        }
        isFirstTimeTwo = false;
        executor.execute(new Runnable() {
            @Override
            public void run() {
                while (isConnectedTwo) {    //只有第一次连接才需要自己手动连接，后面系统会自动恢复连接
                    try {
                        rabbitMQ.receiveQueueRoutingKeyMessage(queueName, routingKey, new RabbitMQClient.ReceiveMessageListener() {
                            @Override
                            public void receive(String message) {
                                if (listener != null) listener.receiveMessage(message);
                            }

                        });
                        isConnectedTwo = false;
                    } catch (IOException | TimeoutException e) {
                        isConnectedTwo = true;
                        e.printStackTrace();
                        SystemClock.sleep(5000);  //等待五秒在连接
                        Log.d(TAG, "未连接到routingkey");
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
        isConnectedOne = true;
        isConnectedTwo = true;
        isFirstTimeOne = true;
        isFirstTimeTwo = true;
        Log.d(TAG, "关闭推送");
    }

    public interface ReceiveMessageListener {
        void receiveMessage(String message);
    }

    public interface SendMessageListener {
        void sendMessage(boolean isSuccess);
    }


}
