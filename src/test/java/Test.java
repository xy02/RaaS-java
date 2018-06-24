import com.github.xy02.raas.RaaSNode;
import com.github.xy02.raas.nats.NatsNode;
import io.reactivex.Observable;
import io.reactivex.schedulers.Schedulers;

import java.util.concurrent.TimeUnit;

public class Test {
    public static long read = 0;
    public static long secondsAgo = 0;

    public static void main(String[] args) {
        try {
            RaaSNode node = new NatsNode();
            //register service
            node.register("test.s1", ctx -> ctx.getInputData()
//                            Observable.interval(0, 1, TimeUnit.NANOSECONDS)
//                                    .map(x -> ("1" + x).getBytes())
//                            .doOnNext(x -> read++)
//                            .flatMap(x -> Observable.never())
                            .map(x -> new String(x))
//                            .doOnNext(System.out::println)
                            .map(x -> x + " OK")
                            .map(String::getBytes)
            )
                    .subscribe();

            //register service
            node.register("test.s2", ctx ->
                            node.call("test.s1", ctx.getInputData())
                                    .map(x -> new String(x))
//                            .doOnNext(System.out::println)
                                    .map(x -> x + " on s2")
                                    .map(String::getBytes)
            )
                    .subscribe();

            //log
            long sample = 2;
            Observable.interval(1, TimeUnit.SECONDS, Schedulers.io())
                    .sample(sample, TimeUnit.SECONDS)
                    .doOnNext(x -> System.out.printf("%d sec read: %d, ops: %d/s\n", x + 1, read, (read - secondsAgo) / sample))
                    .doOnNext(x -> secondsAgo = read)
//                    .subscribe(x -> System.out.printf("%d sec read: %d, ops: %d/s\n", x + 1, read, read / (x + 1)));
                    .subscribe();

            //call service
//            NatsNode node2 = new NatsNode(new Options());
            byte[] buf = "hello".getBytes();
            node.call("test.s2",
                    Observable.interval(0, 2, TimeUnit.MICROSECONDS)
                            .doOnSubscribe(d -> System.out.println("doOnSubscribe"))
                            .map(x -> buf)
            )
                    .doOnNext(x -> read++)
                    .map(x -> new String(x))
//                    .doOnNext(System.out::println)
                    .subscribe();

            //forever
            Thread.sleep(Long.MAX_VALUE);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
