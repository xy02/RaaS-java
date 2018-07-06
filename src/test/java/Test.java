import com.github.xy02.raas.RaaSNode;
import com.github.xy02.raas.nats.NatsNode;
import io.reactivex.Observable;

import java.util.concurrent.TimeUnit;

public class Test {


    public static void main(String[] args) {
        try {
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

            //forever
            Thread.sleep(Long.MAX_VALUE);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
