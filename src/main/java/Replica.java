import com.rabbitmq.client.*;

import java.io.FileWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class Replica {
    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.out.println("Usage: java Replica <replicaNumber>");
            return;
        }
        int replicaNumber = Integer.parseInt(args[0]);
        String queueName = "replica" + replicaNumber;
        String filename = "replica" + replicaNumber + ".txt";

        Connection connection = RabbitMQUtils.getConnection();
        Channel channel = RabbitMQUtils.createChannel(connection);

        String exchangeName = "replica_exchange";

        // Declare exchange
        channel.exchangeDeclare(exchangeName, "fanout", false, true, null);

        // Declare auto-delete, non-durable queue
        channel.queueDeclare(queueName, false, false, true, null);

        // Bind the queue to the fanout exchange
        channel.queueBind(queueName, exchangeName, "");

        System.out.println("Replica" + replicaNumber + " waiting for messages...");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println("Replica" + replicaNumber + " received: " + message);
            try (FileWriter write = new FileWriter(filename, true)) {
                write.write(message + "\n");
            }
        };
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {});

        // read_requests logic unchanged
        String requestQueue = "read_requests_" + replicaNumber;
        channel.queueDeclare(requestQueue, false, false, true, null);
        channel.exchangeDeclare("read_broadcast", BuiltinExchangeType.FANOUT);
        channel.queueBind(requestQueue, "read_broadcast", "");


        DeliverCallback readCallback = (consumerTag, delivery) -> {
            String command = new String(delivery.getBody(), StandardCharsets.UTF_8);

            if ("Read Last".equalsIgnoreCase(command)) {
                String lastLine = RabbitMQUtils.getLastLineFromFile(filename);
                AMQP.BasicProperties replyProps = new AMQP.BasicProperties
                        .Builder()
                        .correlationId(delivery.getProperties().getCorrelationId())
                        .build();
                channel.basicPublish("", delivery.getProperties().getReplyTo(), replyProps, lastLine.getBytes(StandardCharsets.UTF_8));
            }

            if ("Read All".equalsIgnoreCase(command)) {
                List<String> lines = RabbitMQUtils.getAllLinesFromFile(filename);
                for (String line : lines) {
                    AMQP.BasicProperties replyProps = new AMQP.BasicProperties
                            .Builder()
                            .correlationId(delivery.getProperties().getCorrelationId())
                            .build();
                    channel.basicPublish("", delivery.getProperties().getReplyTo(), replyProps, line.getBytes(StandardCharsets.UTF_8));
                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        };

        channel.basicConsume(requestQueue, true, readCallback, consumerTag -> {});
    }
}
