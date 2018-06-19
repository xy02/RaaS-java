import io.nats.client.Connection;
import io.nats.client.Message;
import io.nats.client.MessageHandler;
import io.nats.client.Nats;

import java.io.IOException;

public class Test {
    public static void main(String[] args) {
        try {
            Connection nc = Nats.connect();
            nc.subscribe("test", msg->{

            });


            Thread.sleep(Long.MAX_VALUE);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
