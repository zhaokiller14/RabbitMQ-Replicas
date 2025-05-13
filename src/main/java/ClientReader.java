import com.rabbitmq.client.*;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.List;
import java.util.ArrayList;
public class ClientReader {
    private static final String REQUEST_QUEUE = "read_requests";

    public static void main(String[] args) throws Exception {
        try (Connection connection = RabbitMQUtils.getConnection()) {
            Channel channel = RabbitMQUtils.createChannel(connection);

            // Prepare request
            String correlationId = UUID.randomUUID().toString();
            String replyQueue = channel.queueDeclare().getQueue();

            AMQP.BasicProperties props = new AMQP.BasicProperties
                    .Builder()
                    .correlationId(correlationId)
                    .replyTo(replyQueue)
                    .build();

            channel.exchangeDeclare("read_broadcast", BuiltinExchangeType.FANOUT);
            channel.basicPublish("read_broadcast", "", props, "Read Last".getBytes(StandardCharsets.UTF_8));

            System.out.println("Sent 'Read Last' request.");

            final boolean[] responseReceived = {false};

            List<String> responses = new ArrayList<>();
            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                if (delivery.getProperties().getCorrelationId().equals(correlationId)) {
                    String response = new String(delivery.getBody(), StandardCharsets.UTF_8);
                    responses.add(response);
                    System.out.println("Received response: " + response);
                }
            };

            channel.basicConsume(replyQueue, true, deliverCallback, consumerTag -> {});

            Thread.sleep(2000);

            System.out.println("\nAll 'Last' messages from replicas:");
            for (String r : responses) {
                System.out.println("- " + r);
            }

        }
    }
}
