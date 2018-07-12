import com.github.xy02.raas.RaaSNode;
import com.github.xy02.raas.nats.NatsNode;
import io.reactivex.Observable;

import java.util.concurrent.TimeUnit;

public class UnaryClient {
    public static long read = 0;
    public static long secondsAgo = 0;

    public static void main(String[] args) {
        try {
            //call service
            RaaSNode node = new NatsNode();
            byte[] buf = "hello".getBytes();
            Observable.interval(0, 5, TimeUnit.MICROSECONDS)
//                    .doOnNext(x -> System.out.println(Thread.currentThread().getName()))
                    .flatMapSingle(x ->
                            node.callUnaryService("test.s1", buf, 1, TimeUnit.SECONDS)
                    )
//                    .doOnNext(x -> System.out.println(Thread.currentThread().getName()))
                    .doOnNext(x -> read++)
//                    .doOnNext(x -> System.out.println(new String(x)))
                    .subscribe(x -> {
                    }, err -> System.out.println(err.getMessage()));
//
//            node.call("test.s3",
//                    Observable.interval(0, 70, TimeUnit.MICROSECONDS)
//                            .doOnSubscribe(d -> System.out.println("doOnSubscribe"))
//                            .map(x -> buf)
//            )
////                    .doOnNext(x -> System.out.println(new String(x)))
//                    .doOnNext(x -> read++)
//                    .subscribe(x->{},err->System.out.println(err.getMessage()));
//
//
//            //log
            long sample = 2;
            Observable.interval(1, TimeUnit.SECONDS)
                    .sample(sample, TimeUnit.SECONDS)
                    .doOnNext(x -> System.out.printf("%d sec read: %d, ops: %d/s\n", x + 1, read, (read - secondsAgo) / sample))
                    .doOnNext(x -> secondsAgo = read)
                    //                    .subscribe(x -> System.out.printf("%d sec read: %d, ops: %d/s\n", x + 1, read, read / (x + 1)));
                    .subscribe();

            //forever
            Thread.sleep(Long.MAX_VALUE);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
