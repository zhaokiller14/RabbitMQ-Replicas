import com.rabbitmq.client.*;

import java.nio.charset.StandardCharsets;
import java.util.*;

public class ClientReaderV2 {
    private static final String REQUEST_QUEUE = "read_requests";

    public static void main(String[] args) throws Exception {
        Connection connection = RabbitMQUtils.getConnection();
        Channel channel = RabbitMQUtils.createChannel(connection);

        String correlationId = UUID.randomUUID().toString();
        String replyQueue = channel.queueDeclare().getQueue();

        AMQP.BasicProperties props = new AMQP.BasicProperties
                .Builder()
                .correlationId(correlationId)
                .replyTo(replyQueue)
                .build();

        channel.exchangeDeclare("read_broadcast", BuiltinExchangeType.FANOUT);
        channel.basicPublish("read_broadcast", "", props, "Read All".getBytes(StandardCharsets.UTF_8));
        System.out.println("Sent 'Read All' request.");

        List<String> allLines = new ArrayList<>();
        long startTime = System.currentTimeMillis();

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            if (delivery.getProperties().getCorrelationId().equals(correlationId)) {
                String line = new String(delivery.getBody(), StandardCharsets.UTF_8);
                allLines.add(line);
                System.out.println("Received: " + line);
            }
        };

        channel.basicConsume(replyQueue, true, deliverCallback, consumerTag -> {});
        System.out.println("ALL:"+allLines);
        // Wait for responses (e.g., 3 seconds max)
        while (System.currentTimeMillis() - startTime < 3000) {
            Thread.sleep(100);
        }
        System.out.println("ALL:"+allLines);
        // Compute majority
        Map<String, Integer> freq = new HashMap<>();
        for (String line : allLines) {
            freq.put(line, freq.getOrDefault(line, 0) + 1);
        }

// Find the maximum frequency
        int maxFreq = freq.values().stream().max(Integer::compareTo).orElse(0);=)
// Print lines that have the maximum frequency
        System.out.println("\nLines with highest frequency (" + maxFreq + "):");
        for (Map.Entry<String, Integer> entry : freq.entrySet()) {
            if (entry.getValue() == maxFreq) {
                System.out.println(entry.getKey());
            }
        }


        channel.close();
        connection.close();
    }
}
