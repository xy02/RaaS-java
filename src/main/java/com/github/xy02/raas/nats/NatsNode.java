package com.github.xy02.raas.nats;

import com.github.xy02.nats.Connection;
import com.github.xy02.nats.IConnection;
import com.github.xy02.nats.MSG;
import com.github.xy02.raas.*;
import com.google.protobuf.ByteString;
import io.reactivex.Observable;
import io.reactivex.Observer;
import io.reactivex.Single;
import io.reactivex.subjects.PublishSubject;
import io.reactivex.subjects.Subject;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class NatsNode implements RaaSNode {
    private static byte[] finalCompleteMessage = DataOuterClass.Data.newBuilder().setFinal("").build().toByteArray();
    private static byte[] pingMessage = DataOuterClass.Data.newBuilder().setPingPong(true).build().toByteArray();
    private static byte[] pongMessage = DataOuterClass.Data.newBuilder().setPingPong(false).build().toByteArray();

    //service connection
    private IConnection serviceConn;
    //client connection
    private IConnection clientConn;

    private RaaSOptions options;

    public NatsNode(RaaSOptions options) throws IOException {
        this.options = options;
        serviceConn = new Connection(options.getNatsOptions());
        clientConn = serviceConn;
        if (!options.isSingleSocket())
            clientConn = new Connection(options.getNatsOptions());
    }

    public NatsNode() throws IOException {
        this(new RaaSOptions());
    }

    @Override
    public Observable<ServiceInfo> registerUnaryService(String serviceName, UnaryService service) {
        IConnection conn = serviceConn;
        Subject<ServiceInfo> serviceInfoSubject = PublishSubject.create();
        ServiceInfo info = new ServiceInfo();
        return serviceConn
                .subscribeMsg("us." + serviceName, "service")
                .flatMapSingle(msg -> Single.just(DataOuterClass.Data.parseFrom(msg.getBody()))
                        .flatMap(data -> service.onCall(new NatsContext<>(data.getRaw().toByteArray(), this))
                                .map(result -> DataOuterClass.Data.newBuilder()
                                        .setRaw(ByteString.copyFrom(result))
//                                        .setRequestId(data.getRequestId())
                                        .build().toByteArray()
                                )
                                .doOnError(err -> {
                                    info.errorNum++;
                                    serviceInfoSubject.onNext(info);
                                })
                                .onErrorReturn(t -> DataOuterClass.Data.newBuilder()
                                        .setFinal(t.getMessage())
//                                        .setRequestId(data.getRequestId())
                                        .build().toByteArray()
                                )
                                .doOnSuccess(result -> conn.publish(new MSG(msg.getReplyTo(), result)))
                                .doOnSuccess(x -> {
                                    info.completedNum++;
                                    serviceInfoSubject.onNext(info);
                                })
                                .doOnSubscribe(x -> {
                                    info.calledNum++;
                                    serviceInfoSubject.onNext(info);
                                })
                        )
                        .onErrorReturnItem(new byte[]{})
                )
                .ofType(ServiceInfo.class)
                .mergeWith(serviceInfoSubject)
                .doFinally(serviceInfoSubject::onComplete)
                ;
    }

    @Override
    public Single<byte[]> unaryCall(String serviceName, byte[] outputData, long timeout, TimeUnit timeUnit) {
        IConnection conn = clientConn;
        byte[] body = DataOuterClass.Data.newBuilder()
                .setRaw(ByteString.copyFrom(outputData))
                .build()
                .toByteArray();
        return conn.request("us." + serviceName, body, timeout, timeUnit)
                .map(msg -> DataOuterClass.Data.parseFrom(msg.getBody()))
                .flatMap(data -> Single.create(emitter -> {
                    switch (data.getTypeCase()) {
                        case RAW:
                            emitter.onSuccess(data.getRaw().toByteArray());
                            break;
                        case FINAL:
                            String err = data.getFinal();
                            if (err == null)
                                err = "no message";
                            emitter.tryOnError(new Exception(err));
                            break;
                        default:
                            throw new Exception("wrong data type");
                    }
                }));
    }

    @Override
    public Observable<ServiceInfo> registerService(String serviceName, Service service) {
        Subject<ServiceInfo> serviceInfoSubject = PublishSubject.create();
        ServiceInfo info = new ServiceInfo();
        return serviceConn
                .subscribeMsg("s." + serviceName, "service")
                .flatMap(handshakeMsg -> onServiceConnected(service, handshakeMsg)
                        .doOnComplete(() -> {
                            info.completedNum++;
                            serviceInfoSubject.onNext(info);
                        })
                        .doOnError(err -> {
                            info.errorNum++;
                            serviceInfoSubject.onNext(info);
                        })
                        .doOnSubscribe(x -> {
                            info.calledNum++;
                            serviceInfoSubject.onNext(info);
                        })
                        .onErrorResumeNext(Observable.empty())
                        .ofType(ServiceInfo.class)
                ).mergeWith(serviceInfoSubject)
                .doFinally(serviceInfoSubject::onComplete)
                ;
    }

    @Override
    public Observable<byte[]> call(String serviceName, Observable<byte[]> outputData) {
        String clientPort = Utils.randomID();
        IConnection conn = clientConn;
        Subject<Boolean> onPingSubject = PublishSubject.create();
        Observable<byte[]> out = conn.request("s." + serviceName, clientPort.getBytes(), options.getHandshakeTimeout(), TimeUnit.SECONDS)
                .map(msg -> new String(msg.getBody()))
                .doOnSuccess(servicePort -> doOnPing(onPingSubject, conn, servicePort))
//                .doOnSuccess(servicePort -> System.out.println("servicePort:" + servicePort))
                .flatMapObservable(servicePort -> outputData
                        .doOnNext(body -> outputNext(conn, servicePort, body))
                        .doOnComplete(() -> outputComplete(conn, servicePort))
                        .doOnError(err -> outputError(conn, servicePort, err))
                        .doOnDispose(() -> outputError(conn, servicePort, new Exception("dispose")))
                )
                .filter(x -> false);
        return observeInputData(conn, clientPort, onPingSubject)
                .mergeWith(out)
                .doFinally(onPingSubject::onComplete);
    }

    private void doOnPing(Observable<Boolean> onPingSubject, IConnection conn, String servicePort) {
        onPingSubject
//                .doOnNext(x -> System.out.println("onPing"))
                .doOnNext(x -> conn.publish(new MSG(servicePort, pongMessage)))
                .subscribe();
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

    private Observable<byte[]> observeInputData(IConnection conn, String port, Observer<Boolean> pingPongSubject) {
        return conn.subscribeMsg(port)
                .timeout(options.getInputTimeout(), TimeUnit.SECONDS)
//                .doOnSubscribe(d->System.out.println("SUB"))
                .map(MSG::getBody)
                .map(DataOuterClass.Data::parseFrom)
                .takeUntil(data -> data.getTypeCase().getNumber() == 2)
                .flatMap(data -> Observable.<byte[]>create(emitter -> {
                    switch (data.getTypeCase().getNumber()) {
                        case 1:
                            emitter.onNext(data.getRaw().toByteArray());
                            break;
                        case 2:
                            String err = data.getFinal();
                            if (err != null && !err.isEmpty())
                                throw new Exception(err);
                        case 3:
                            pingPongSubject.onNext(data.getPingPong());
                            break;
                    }
                    emitter.onComplete();
                }))
                ;
    }

//    //temporarily emit output data
//    private Observable<byte[]> onServiceConnected(Service service, MSG handshakeMsg) {
//        IConnection conn = serviceConn;
//        String clientPort = new String(handshakeMsg.getBody());
////        System.out.println("clientPort:" + clientPort);
//        String servicePort = Utils.randomID();
//        MSG replyMsg = new MSG(handshakeMsg.getReplyTo(), servicePort.getBytes());
//        Subject<byte[]> inputSubject = PublishSubject.create();
//        Subject<Boolean> onPongSubject = PublishSubject.create();
//        Observable<byte[]> inputData = observeInputData(conn, servicePort, onPongSubject)
//                .doOnNext(inputSubject::onNext)
//                .doOnComplete(inputSubject::onComplete)
//                .doOnError(inputSubject::onError)
//                .doOnDispose(inputSubject::onComplete)
//                .mergeWith(Observable.create(emitter -> {
//                    conn.publish(replyMsg);
//                    emitter.onComplete();
//                }))
//                .takeUntil(inputSubject.filter(x->false))
//                ;
//        return service.onCall(new NatsContext<>(inputSubject, this))
//                .doOnNext(raw -> outputNext(conn, clientPort, raw))
//                .doOnComplete(() -> outputComplete(conn, clientPort))
//                .doOnError(err -> outputError(conn, clientPort, err))
//                .doFinally(inputSubject::onComplete)
//                .doFinally(onPongSubject::onComplete)
//                .onErrorResumeNext(Observable.empty())
//                .mergeWith(inputData)
//                .mergeWith(intervalPing(conn, clientPort, onPongSubject))
////                .doFinally(() -> System.out.println("call on final"))
//                ;
//    }

    //temporarily emit output data
    private Observable<byte[]> onServiceConnected(Service service, MSG handshakeMsg) {
        IConnection conn = serviceConn;
        String clientPort = new String(handshakeMsg.getBody());
//        System.out.println("clientPort:" + clientPort);
        String servicePort = Utils.randomID();
        MSG replyMsg = new MSG(handshakeMsg.getReplyTo(), servicePort.getBytes());
        Subject<Boolean> onPongSubject = PublishSubject.create();
        Observable<byte[]> inputData = observeInputData(conn, servicePort, onPongSubject)
                .mergeWith(Observable.create(emitter -> {
                    conn.publish(replyMsg);
                    emitter.onComplete();
                }))
                .takeUntil(onPongSubject.filter(x -> false));
        return service.onCall(new NatsContext<>(inputData, this))
                .doOnNext(raw -> outputNext(conn, clientPort, raw))
                .doOnComplete(() -> outputComplete(conn, clientPort))
                .doOnError(err -> outputError(conn, clientPort, err))
                .doFinally(onPongSubject::onComplete)
                .onErrorResumeNext(Observable.empty())
                .mergeWith(intervalPing(conn, clientPort, onPongSubject))
//                .doFinally(() -> System.out.println("call on final"))
                ;
    }

    private Observable<byte[]> intervalPing(IConnection conn, String clientPort, Observable<Boolean> onPongSubject) {
        Observable<Boolean> ping = onPongSubject
//                .doOnNext(x -> System.out.println("onPong"))
                .take(1)
                .mergeWith(Observable.create(emitter -> {
                    conn.publish(new MSG(clientPort, pingMessage));
                    emitter.onComplete();
                }))
                .timeout(options.getHandshakeTimeout(), TimeUnit.SECONDS);
        return Observable.interval(options.getPingInterval(), TimeUnit.SECONDS)
                .flatMap(x -> ping)
                .takeUntil(onPongSubject.filter(x -> false))
                .ofType(byte[].class);
    }

    private void outputNext(IConnection conn, String port, byte[] raw) throws IOException {
        DataOuterClass.Data data = DataOuterClass.Data.newBuilder().setRaw(ByteString.copyFrom(raw)).build();
        conn.publish(new MSG(port, data.toByteArray()));
    }

    private void outputComplete(IConnection conn, String port) throws IOException {
        conn.publish(new MSG(port, finalCompleteMessage));
    }

    private void outputError(IConnection conn, String port, Throwable t) throws IOException {
        byte[] body = DataOuterClass.Data.newBuilder().setFinal(t.getMessage()).build().toByteArray();
        conn.publish(new MSG(port, body));
    }

}
