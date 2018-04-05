package com.app.harry.rabbitmq_android;

import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.text.TextUtils;
import android.text.method.ScrollingMovementMethod;
import android.view.View;
import android.widget.EditText;
import android.widget.TextView;
import android.widget.Toast;

public class MainActivity extends AppCompatActivity {

    private TextView showMessageTv;
    private EditText contentEt;
    private StringBuilder sb = new StringBuilder();

    //自己配置发送接受队列名称和绑定key
    private final String sendQueueOne = "testOne";
    private final String sendRoutingKey = "two.rabbit.ok";
    private final String receiveQueueOne = "testOne";
    private final String receiveQueueTwo = "testTwo";
    private final String receiveRoutingKey = "two.#";

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);
        contentEt = findViewById(R.id.et_content);
        showMessageTv = findViewById(R.id.tv_show_message);
        showMessageTv.setMovementMethod(ScrollingMovementMethod.getInstance());

        RabbitMQUtil.initService("192.168.2.121", 5672, "ai", "ai");
        RabbitMQUtil.initExchange("test", "topic");
    }

    @Override
    protected void onDestroy() {
        RabbitMQUtil.getInstance().close();
        super.onDestroy();
    }

    public void sendQueue(View view) {
        final String message = contentEt.getText().toString().trim();

        if (TextUtils.isEmpty(message)) {
            Toast.makeText(MainActivity.this, "消息不能为空！！！", Toast.LENGTH_SHORT).show();
            return;
        }

        RabbitMQUtil.getInstance().sendQueueMessage(message, sendQueueOne, new RabbitMQUtil.SendMessageListener() {
            @Override
            public void sendMessage(final boolean isSuccess) {
                runOnUiThread(new Runnable() {
                    @Override
                    public void run() {
                        if (isSuccess) {
                            sb.append("发送队列消息：").append(message).append("\n");
                            showMessageTv.setText(sb);
                            contentEt.setText("");

                        } else {
                            Toast.makeText(MainActivity.this, "发送消息失败，请检查网络后稍后再试！！！", Toast.LENGTH_SHORT).show();
                        }
                    }
                });


            }
        });
    }

    public void sendRouting(View view) {
        final String message = contentEt.getText().toString().trim();

        if (TextUtils.isEmpty(message)) {
            Toast.makeText(MainActivity.this, "消息不能为空！！！", Toast.LENGTH_SHORT).show();
            return;
        }

        RabbitMQUtil.getInstance().sendRoutingKeyMessage(message, sendRoutingKey, new RabbitMQUtil.SendMessageListener() {
            @Override
            public void sendMessage(final boolean isSuccess) {
                runOnUiThread(new Runnable() {
                    @Override
                    public void run() {
                        if (isSuccess) {
                            sb.append("发送routing消息：").append(message).append("\n");
                            contentEt.setText("");
                            showMessageTv.setText(sb);

                        } else {
                            Toast.makeText(MainActivity.this, "发送消息失败，请检查网络后稍后再试！！！", Toast.LENGTH_SHORT).show();
                        }
                    }
                });
            }
        });
    }

    public void listenQueue(View view) {
        RabbitMQUtil.getInstance().receiveQueueMessage(receiveQueueOne, new RabbitMQUtil.ReceiveMessageListener() {
            @Override
            public void receiveMessage(String message) {
                sb.append("收到了queue消息：").append(message).append("\n");
                runOnUiThread(new Runnable() {
                    @Override
                    public void run() {
                        showMessageTv.setText(sb);
                    }
                });
            }
        });
    }

    public void listenRouting(View view) {
        RabbitMQUtil.getInstance().receiveRoutingKeyMessage(receiveRoutingKey, new RabbitMQUtil.ReceiveMessageListener() {
            @Override
            public void receiveMessage(String message) {
                sb.append("收到了routing消息：").append(message).append("\n");
                runOnUiThread(new Runnable() {
                    @Override
                    public void run() {
                        showMessageTv.setText(sb);
                    }
                });
            }
        });
    }
}
