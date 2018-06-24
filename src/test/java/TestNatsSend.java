import io.nats.client.Connection;
import io.nats.client.Nats;
import io.reactivex.Observable;
import io.reactivex.schedulers.Schedulers;

import java.util.concurrent.TimeUnit;

public class TestNatsSend {

    static long read;

    public static void main(String[] args) {
        try {

            byte[] data = "hello".getBytes();
            Connection nc = Nats.connect();

            Observable.create(emitter -> {
                Connection nc2 = Nats.connect();
                nc2.subscribe("test", msg -> {
                    System.out.println(msg.getReplyTo());
                    read++;
                });
            }).subscribeOn(Schedulers.io()).subscribe();

            nc.request("test",data);


//            Observable.create(emitter1 -> {
//                System.out.printf("publish on 1 :%s\n", Thread.currentThread().getName());
//                while (true) {
//                    nc.publish("sub1", data);
//                    //Thread.sleep(1000);
//                }
//            }).subscribeOn(Schedulers.io()).subscribe();
//
//            Observable.interval(1,TimeUnit.SECONDS)
//                    .subscribe(x -> System.out.printf("%d sec read: %d, ops: %d/s\n", x + 1, read, read / (x + 1)));

//            Thread.sleep(Long.MAX_VALUE);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
