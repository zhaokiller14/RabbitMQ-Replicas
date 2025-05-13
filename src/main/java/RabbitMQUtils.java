import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Channel;
import java.util.List;
import java.util.ArrayList;
import java.util.Scanner;
import java.io.File;
import java.io.FileNotFoundException;

public class RabbitMQUtils {
    private static final String HOST = "localhost";

    public static Connection getConnection() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(HOST);
        return factory.newConnection();
    }
    public static Channel channel (Connection connection) throws Exception {
        return connection.createChannel();
    }
    public static String getLastLineFromFile(String filename) {
        try (java.io.RandomAccessFile file = new java.io.RandomAccessFile(filename, "r")) {
            long length = file.length();
            if (length == 0) return "";
            long pos = length - 1;
            while (pos > 0) {
                pos--;
                file.seek(pos);
                if (file.readByte() == '\n') break;
            }
            if (pos == 0) file.seek(0);
            return file.readLine();
        } catch (Exception e) {
            e.printStackTrace();
            return "";
        }
    }
    public static Channel createChannel(Connection connection) throws Exception {
        Channel channel = connection.createChannel();
        return channel;
    }

    public static Connection createConnection() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        return factory.newConnection();
    }
    public static List<String> getAllLinesFromFile(String filename) {
        List<String> lines = new ArrayList<>();
        try (Scanner scanner = new Scanner(new File(filename))) {
            while (scanner.hasNextLine()) {
                lines.add(scanner.nextLine());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return lines;
    }

}