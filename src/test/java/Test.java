import com.github.xy02.raas.RaaSNode;
import com.github.xy02.raas.nats.NatsNode;
import io.reactivex.Observable;

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
                    ctx -> {
                        System.out.println("tid"+Thread.currentThread().getId());
                        return Observable.interval(0, 1, TimeUnit.SECONDS)
//                            .doOnNext(x -> System.out.println(new String(x)))
                                .map(x -> new String(ctx.getRequestBin()) + x)
                                .map(String::getBytes);
                    }
            )
                    .doOnNext(x -> System.out.printf("onCall: %d, onError: %d, onComplete: %d\n", x.calledNum, x.errorNum, x.completedNum))
                    .subscribe();

            //forever
            Thread.sleep(Long.MAX_VALUE);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
