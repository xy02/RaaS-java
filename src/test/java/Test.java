import com.github.xy02.raas.RaaSNode;
import com.github.xy02.raas.nats.NatsNode;
import io.reactivex.Observable;
import io.reactivex.Single;

import java.util.concurrent.TimeUnit;

public class Test {


    public static void main(String[] args) {
        try {
            RaaSNode node = new NatsNode();
            //registerService service
            node.registerService("test.s1", ctx -> ctx.getInputData()
//                            .doOnNext(x -> System.out.println(new String(x)))
                            .map(x -> new String(x) + " OK")
                            .map(String::getBytes)
            )
                    .doOnNext(x -> System.out.printf("onCall: %d, onError: %d, onComplete: %d\n", x.calledNum, x.errorNum, x.completedNum))
                    .subscribe();

            //registerService service
            node.registerService("test.s2", ctx ->ctx.getInputData()
                    .flatMap(x-> node.call("test.s4", Observable.just(x)))
//                            .map(String::getBytes)
//                            .doOnNext(x -> System.out.println(new String(x)))
//                                    .map(x -> new String(x) + " on s2")

            )
                    .subscribe();

            //registerService service
            node.registerService("test.s3", ctx -> ctx.getInputData()
//                    .doOnNext(x -> System.out.println(Thread.currentThread().getName()))
                    .flatMapSingle(x -> ctx.unaryCall("test.s1", x, 1, TimeUnit.SECONDS))
//                    .doOnNext(x -> System.out.println(Thread.currentThread().getName()))
            )
                    .subscribe();

            //registerService service
            node.registerService("test.s4", ctx ->ctx.getInputData()
                            .take(1)
//                    .doOnComplete(()->System.out.println("complete"))
//                            .doOnNext(x -> System.out.println(new String(x)))
                    .map(x -> new String(x) + " on s4")
                    .map(String::getBytes)
            )
                    .subscribe();

            //registerService unary service
            node.registerUnaryService("test.s1", ctx -> Single.<String>create(emitter -> {
                        emitter.onSuccess(new String(ctx.getInputData()) + " OK2");
//                        emitter.tryOnError(new Exception("e..."));
                    })
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
