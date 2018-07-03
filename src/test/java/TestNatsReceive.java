import io.nats.client.Connection;
import io.nats.client.Message;
import io.nats.client.MessageHandler;
import io.nats.client.Nats;
import io.reactivex.Observable;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class TestNatsReceive {

    static public long read = 0;

    static long secondsAgo = 0;

    public static void main(String[] args) {
        try {
            Connection nc = Nats.connect();
            nc.subscribe("test", msg->{
                try {
                    nc.publish(msg.getReplyTo(),msg.getData());
                } catch (IOException e) {
                    e.printStackTrace();
                }
                read++;
            });
            //log
            long sample = 5;
            Observable.interval(1, TimeUnit.SECONDS)
                    .sample(sample,TimeUnit.SECONDS)
                    .doOnNext(x->  System.out.printf("%d sec read: %d, ops: %d/s\n", x + 1, read, (read-secondsAgo) / sample))
                    .doOnNext(x-> secondsAgo = read)
                    .subscribe();

            Thread.sleep(Long.MAX_VALUE);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
