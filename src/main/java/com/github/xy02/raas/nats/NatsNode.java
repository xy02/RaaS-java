package com.github.xy02.raas.nats;

import com.github.xy02.nats.Connection;
import com.github.xy02.nats.IConnection;
import com.github.xy02.nats.MSG;
import com.github.xy02.raas.RaaSNode;
import com.github.xy02.raas.Service;
import com.github.xy02.raas.ServiceInfo;
import com.github.xy02.raas.UnaryService;
import io.reactivex.Observable;
import io.reactivex.Single;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class NatsNode implements RaaSNode {

    private Client client;

    private Server server;

    private RaaSOptions options;

    private IConnection clientConn;

    public NatsNode(RaaSOptions options) throws IOException {
        this.options = options;
        IConnection serverConn = new Connection(options.getNatsOptions());
        IConnection clientConn = serverConn;
        if (!options.isSingleSocket())
            clientConn = new Connection(options.getNatsOptions());

        client = new Client(clientConn, options);
        server = new Server(serverConn, this, options);
        this.clientConn = clientConn;
    }

    public NatsNode() throws IOException {
        this(new RaaSOptions());
    }

    @Override
    public Observable<byte[]> callService(String serviceName, byte[] outputBin) {
        return client.callService(serviceName, outputBin);
    }

    @Override
    public Observable<ServiceInfo> registerService(String serviceName, Service service) {
        return server.registerService(serviceName, service);
    }

//    @Override
//    public Observable<ServiceInfo> registerUnaryService(String serviceName, UnaryService service) {
//        IConnection conn = serviceConn;
//        Subject<ServiceInfo> serviceInfoSubject = PublishSubject.create();
//        ServiceInfo info = new ServiceInfo();
//        return serviceConn
//                .subscribeMsg("us." + serviceName, "service")
//                .doOnNext(msg -> {
//                    DataOuterClass.Data data = DataOuterClass.Data.parseFrom(msg.getBody());
//                    msg.setBody(data.getRaw().toByteArray());
//                    msg.setReplyTo(data.getReply());
//                })
//                .flatMapSingle(msg -> service.onCall(new NatsContext<>(msg.getBody(), this))
//                        .map(raw -> DataOuterClass.Data.newBuilder().setRaw(ByteString.copyFrom(raw)).build().toByteArray())
//                        .doOnError(err -> {
//                            info.errorNum++;
//                            serviceInfoSubject.onNext(info);
//                        })
//                        .onErrorReturn(t -> DataOuterClass.Data.newBuilder().setFinal(t.getMessage()).build().toByteArray())
//                        .doOnSuccess(result -> conn.publish(new MSG(msg.getReplyTo(), result)))
//                        .doOnSuccess(x -> {
//                            info.completedNum++;
//                            serviceInfoSubject.onNext(info);
//                        })
//                        .doOnSubscribe(x -> {
//                            info.calledNum++;
//                            serviceInfoSubject.onNext(info);
//                        })
//                        .onErrorReturnItem(new byte[]{})
//                )
//                .ofType(ServiceInfo.class)
//                .mergeWith(serviceInfoSubject)
//                .doFinally(serviceInfoSubject::onComplete)
//                ;
//    }
//
//    @Override
//    public Single<byte[]> unaryCall(String serviceName, byte[] outputData, long timeout, TimeUnit timeUnit) {
//        IConnection conn = clientConn;
//        String reply = myRequestPrefix + Utils.randomID();
//        return onResponseSubject
//                .filter(msg -> msg.getSubject().equals(reply))
//                .mergeWith(Observable.create(emitter -> {
//                            byte[] body = DataOuterClass.Data.newBuilder()
//                                    .setReply(reply)
//                                    .setRaw(ByteString.copyFrom(outputData))
//                                    .build()
//                                    .toByteArray();
//                            conn.publish(new MSG("us." + serviceName, body));
//                            emitter.onComplete();
//                        })
////                        .doOnComplete(() -> System.out.println(Thread.currentThread().getName()))
//                )
////                .doOnNext(x -> System.out.println(Thread.currentThread().getName()))
//                .take(1)
//                .singleOrError()
//                .timeout(timeout, timeUnit)
//                .map(MSG::getBody)
//                .map(DataOuterClass.Data::parseFrom)
//                .flatMap(data -> Single.create(emitter -> {
//                    switch (data.getTypeCase().getNumber()) {
//                        case 1:
//                            emitter.onSuccess(data.getRaw().toByteArray());
//                            break;
//                        case 2:
//                            emitter.tryOnError(new Exception(data.getFinal()));
//                            break;
//                        default:
//                            throw new Exception("wrong data type");
//                    }
//                }))
//                ;
//    }


    @Override
    public Observable<ServiceInfo> registerUnaryService(String serviceName, UnaryService service) {
        return server.registerUnaryService(serviceName, service);
    }

    @Override
    public Single<byte[]> callUnaryService(String serviceName, byte[] outputBin, long timeout, TimeUnit timeUnit) {
        return client.callUnaryService(serviceName, outputBin, timeout, timeUnit);
    }

    @Override
    public Observable<byte[]> subscribe(String subject) {
        return clientConn.subscribeMsg(subject)
                .map(MSG::getBody);
    }

    @Override
    public void publish(String subject, byte[] data) throws IOException {
        clientConn.publish(new MSG(subject, data));
    }
}
