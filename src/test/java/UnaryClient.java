import com.github.xy02.raas.RaaSNode;
import com.github.xy02.raas.nats.NatsNode;
import io.reactivex.Observable;
import io.reactivex.disposables.Disposable;
import io.reactivex.schedulers.Schedulers;

import java.util.concurrent.TimeUnit;

public class UnaryClient {
    public static long read = 0;
    public static long secondsAgo = 0;

    public static void main(String[] args) {
        try {
            //call service
            RaaSNode node = new NatsNode();
            byte[] buf = "hello".getBytes();
            Observable<Long> one = Observable.interval(2*1000000, 30, TimeUnit.MICROSECONDS);
            Observable<Long> two = Observable.interval(10*1000000, 25, TimeUnit.MICROSECONDS);
//            Observable<Long> three = Observable.interval(15*1000000, 15, TimeUnit.MICROSECONDS);
            Disposable d2 = one
                    .takeUntil(two)
                    .mergeWith(two)
//                    .takeUntil(three)
//                    .mergeWith(three)
//                    .doOnNext(x -> System.out.println(Thread.currentThread().getName()))
                    .flatMapSingle(x ->
                            node.unaryCall("test.s1", buf, 1, TimeUnit.SECONDS)
                    )
//                    .doOnNext(x -> System.out.println(Thread.currentThread().getName()))
                    .doOnNext(x -> read++)
//                    .doOnNext(x -> System.out.println(new String(x)))
                    .subscribe(x -> {
                    }, err -> System.out.println(err.getMessage()));

//            node.call("test.s3",
//                    Observable.interval(0, 100, TimeUnit.MICROSECONDS)
//                            .doOnSubscribe(d -> System.out.println("doOnSubscribe"))
//                            .map(x -> buf)
//            )
////                    .doOnNext(x -> System.out.println(new String(x)))
//                    .doOnNext(x -> read++)
//                    .subscribe(x->{},err->System.out.println(err.getMessage()));


            //log
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
