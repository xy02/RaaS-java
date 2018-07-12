import com.github.xy02.raas.RaaSNode;
import com.github.xy02.raas.nats.NatsNode;
import io.reactivex.Observable;
import io.reactivex.Single;

import java.util.concurrent.TimeUnit;

public class Test {

    public static long read = 0;
    public static long secondsAgo = 0;

    public static void main(String[] args) {
        try {
            //log
            long sample = 2;
            Observable.interval(1, TimeUnit.SECONDS)
                    .sample(sample, TimeUnit.SECONDS)
                    .doOnNext(x -> System.out.printf("%d sec read: %d, ops: %d/s\n", x + 1, read, (read - secondsAgo) / sample))
                    .doOnNext(x -> secondsAgo = read)
                    //                    .subscribe(x -> System.out.printf("%d sec read: %d, ops: %d/s\n", x + 1, read, read / (x + 1)));
                    .subscribe();

            RaaSNode node = new NatsNode();
            //registerService service
            node.registerService("test.s1",
                    (bin, ctx) -> Observable.interval(0, 1, TimeUnit.NANOSECONDS)
//                            .doOnNext(x -> System.out.println(new String(x)))
                            .map(x -> new String(bin) + x)
                            .map(String::getBytes)
            )
                    .doOnNext(x -> System.out.printf("onCall: %d, onError: %d, onComplete: %d\n", x.calledNum, x.errorNum, x.completedNum))
                    .subscribe();


            node.registerUnaryService("test.s1",
                    (bin, ctx) -> Single.just(bin)
//                            .doOnSuccess(x -> System.out.println(new String(x)))
                            .map(x -> new String(x) + "ok")
                            .map(String::getBytes)
            )
//                    .doOnNext(x -> System.out.printf("onCall: %d, onError: %d, onComplete: %d\n", x.calledNum, x.errorNum, x.completedNum))
                    .subscribe();
            //forever
            Thread.sleep(Long.MAX_VALUE);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
