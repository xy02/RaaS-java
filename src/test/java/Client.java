import com.github.xy02.nats.Connection;
import com.github.xy02.nats.IConnection;
import com.github.xy02.nats.MSG;
import com.github.xy02.raas.Data;
import com.github.xy02.raas.RaaSNode;
import com.github.xy02.raas.nats.NatsNode;
import com.google.protobuf.ByteString;
import io.reactivex.Observable;
import io.reactivex.disposables.Disposable;
import io.reactivex.schedulers.Schedulers;

import java.util.concurrent.TimeUnit;

public class Client {
    public static long read = 0;
    public static long secondsAgo = 0;

    public static void main(String[] args) {
        try {
            IConnection serverConn = new Connection();
            //call service
            RaaSNode node = new NatsNode();
            byte[] buf = "hello".getBytes();
            Observable.interval(0, 30, TimeUnit.MICROSECONDS)
                    .flatMap(x->
                            node.callService("test.s5", buf, Observable.empty())
                                    .doOnComplete(() -> read++)
                    )
//                    .flatMap(x->Observable.create(emitter -> {
//                        byte[] req = Data.Request.newBuilder()
//                                .setSessionId("test")
//                                .setClientId("tt")
//                                .setBin(ByteString.copyFrom(buf))
//                                .build().toByteArray();
//                        serverConn.publish(new MSG("test.s5", req));
//                        emitter.onComplete();
//                    }))
//                    .doOnNext(X -> read++)
                    .subscribe();

//            Disposable d2 = node.callService("test.s4", buf,
//                    Observable.interval(100,TimeUnit.MICROSECONDS)
//                    .map(x->buf)
//            )
//                    .doOnNext(x -> read++)
////                    .map(x -> new String(x))
////                    .doOnNext(System.out::println)
//                    .subscribe(x -> {
//                    }, err -> System.out.println(err.getMessage()));


//            serverConn.publish(new MSG("test.s5", req));
//            Observable.interval(1000000, 5, TimeUnit.MILLISECONDS)
////                    .doOnNext(x -> System.out.println(Thread.currentThread().getName()))
//                    .doOnNext(X->{
//                        //send request data
//                        byte[] req = Data.Request.newBuilder()
//                                .setSessionId("test")
//                                .setClientId("tt")
//                                .setBin(ByteString.copyFrom(buf))
//                                .build().toByteArray();
//                        serverConn.publish(new MSG("test.s5", req));
//                    })
////                    .flatMap(x ->
////
////                            node.callService("test.s5", buf, null)
////                    )
////                    .doOnNext(x -> System.out.println(Thread.currentThread().getName()))
//                    .doOnNext(x -> read++)
////                    .doOnNext(x -> System.out.println(new String(x)))
//                    .subscribe(x -> {
//                    }, err -> System.out.println(err.getMessage()));

//            Observable.timer(5, TimeUnit.SECONDS).subscribe(x -> d2.dispose());
//            Observable.timer(10, TimeUnit.SECONDS)
//                    .map(x -> "")
//                    .concatWith(node.call("test.s1",
//                            Observable.interval(0, 20, TimeUnit.MICROSECONDS)
//                                    .doOnSubscribe(d -> System.out.println("doOnSubscribe"))
//                                    .map(x -> buf)
//                            )
//                                    .doOnNext(x -> read++)
//                                    .map(x -> new String(x))
//    //                    .doOnNext(System.out::println)
//                    )
//                    .subscribe(x->{},err->System.out.println(err.getMessage()));

            //log
            long sample = 2;
            Observable.interval(1, TimeUnit.SECONDS, Schedulers.io())
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
