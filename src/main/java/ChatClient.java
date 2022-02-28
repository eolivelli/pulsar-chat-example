import lombok.Data;
import org.apache.pulsar.client.api.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class ChatClient {

    @Data
    public static class Line {
        String sender;
        String message;
    }

    public static void main(String ... args) {
        String clientId = UUID.randomUUID().toString();
        try (PulsarClient client = PulsarClient
                .builder()
                .serviceUrl("pulsar://localhost:6650")
                .build();
             Consumer<Line> consumer = client
                     .newConsumer(Schema.JSON(Line.class))
                     .topic("chat")
                     .subscriptionName(clientId)
                     .subscriptionInitialPosition(SubscriptionInitialPosition.Latest)
                     .messageListener(new MessageListener<Line>() {
                         @Override
                         public void received(Consumer<Line> consumer, Message<Line> msg) {
                             Line line = msg.getValue();
                             if (!clientId.equals(line.sender)) {
                                 System.out.println("[" + line.sender + "] < " + line.message);
                             }
                             consumer.acknowledgeAsync(msg);
                         }
                     })
                     .subscribe();
             Producer<Line> producer = client
                     .newProducer(Schema.JSON(Line.class))
                     .topic("chat")
                     .create();
        ) {
            InputStreamReader reader = new InputStreamReader(System.in, StandardCharsets.UTF_8);
            BufferedReader console = new BufferedReader(reader);
            for (String input = console.readLine();
                 input != null;
                 input = console.readLine()) {
                Line line = new Line();
                line.message = input;
                line.sender = clientId;
                System.out.println("[" + line.sender+ " ] > " + line.message);
                producer.send(line);
            }

        } catch (Exception err) {
            err.printStackTrace();
        }
    }


}
