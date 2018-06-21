import com.github.xy02.nats.Options;
import com.github.xy02.raas.nats.NatsNode;
import io.reactivex.Observable;
import io.reactivex.schedulers.Schedulers;

import java.util.concurrent.TimeUnit;

public class Test {
    public static long read = 0;

    public static void main(String[] args) {
        try {
            NatsNode node = new NatsNode(new Options().setHost("192.168.8.99"));
            //register service
            node.register("test.s1", ctx -> ctx.getInputData()
//                            Observable.interval(0, 1, TimeUnit.NANOSECONDS)
//                                    .map(x -> ("1" + x).getBytes())
//                            .doOnNext(x -> read++)
//                            .flatMap(x -> Observable.never())
                            .map(x -> new String(x))
                            .doOnNext(System.out::println)
                            .map(x -> x + " OK")
                            .map(String::getBytes)
            )
                    .subscribe();

            //register service
            node.register("test.s2", ctx ->
                    node.call("test.s1", ctx.getInputData())
                            .map(x -> new String(x))
                            .doOnNext(System.out::println)
                            .map(x -> x + " on s2")
                            .map(String::getBytes)
            )
                    .subscribe();

            //call service
//            node.call("test.s1",
//                    Observable.<Long>create(emitter -> {
//                        long count = 0;
//                        while (true) {
//                            count++;
//                            emitter.onNext(count);
////                            Thread.sleep(1);
//                        }
//                    }).map(x -> ("i:" + x).getBytes())
////                            .subscribeOn(Schedulers.io())
//            ).map(x -> new String(x))
//                    .doOnNext(x -> read++)
////                    .doOnNext(System.out::println)
//                    .subscribe();

            //log
            Observable.interval(1, TimeUnit.SECONDS, Schedulers.io())
                    .subscribe(x -> System.out.printf("%d sec read: %d, ops: %d/s\n", x + 1, read, read / (x + 1)));


            NatsNode node2 = new NatsNode(new Options().setHost("192.168.8.99"));
            byte[] buf = "hello".getBytes();
            node2.call("test.s2",
                    Observable.interval(0, 1, TimeUnit.MILLISECONDS)
                            .doOnSubscribe(d -> System.out.println("doOnSubscribe"))
//                            .map(x -> ("i:" + x).getBytes())
                            .map(x -> buf)
            )
                    .doOnNext(x -> read++)
                    .map(x -> new String(x))
                    .doOnNext(System.out::println)
                    .subscribe();

            //forever
            Thread.sleep(Long.MAX_VALUE);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
