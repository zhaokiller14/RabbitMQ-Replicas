import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.util.Scanner;

public class ClientWriter {
    private static final String EXCHANGE_NAME = "replica_exchange";

    public static void main(String[] args) throws Exception {
        try (Connection connection = RabbitMQUtils.getConnection()) {
            Channel channel = RabbitMQUtils.createChannel(connection);

            // Declare fanout exchange
            channel.exchangeDeclare(EXCHANGE_NAME, "fanout", false, true, null);

            Scanner scanner = new Scanner(System.in);
            System.out.println("Enter messages to send (type 'exit' to quit):");

            while (true) {
                System.out.print("> ");
                String message = scanner.nextLine();
                if ("exit".equalsIgnoreCase(message)) break;

                channel.basicPublish(EXCHANGE_NAME, "", null, message.getBytes());
                System.out.println("Sent: " + message);
            }

            scanner.close();
        }
    }
}
