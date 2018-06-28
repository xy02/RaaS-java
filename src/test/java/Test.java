import com.github.xy02.raas.RaaSNode;
import com.github.xy02.raas.nats.NatsNode;
import io.reactivex.Observable;
import io.reactivex.disposables.Disposable;
import io.reactivex.schedulers.Schedulers;

import java.util.concurrent.TimeUnit;

public class Test {


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
                    .doOnNext(x->System.out.printf("onCall: %d, onError: %d, onComplete: %d\n",x.calledNum,x.errorNum,x.completedNum))
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




            //forever
            Thread.sleep(Long.MAX_VALUE);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
