package com.github.xy02.raas.nats;

import com.github.xy02.nats.Connection;
import com.github.xy02.nats.IConnection;
import com.github.xy02.nats.MSG;
import com.github.xy02.raas.*;
import com.google.protobuf.ByteString;
import io.reactivex.Observable;
import io.reactivex.Observer;
import io.reactivex.subjects.PublishSubject;
import io.reactivex.subjects.Subject;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class NatsNode implements RaaSNode {
    private static byte[] finalCompleteMessage = DataOuterClass.Data.newBuilder().setFinal("").build().toByteArray();
    private static byte[] pingMessage = DataOuterClass.Data.newBuilder().setPing(true).build().toByteArray();
    private static byte[] pongMessage = DataOuterClass.Data.newBuilder().setPong(true).build().toByteArray();

    //service connection
    private IConnection serviceConn;
    //client connection
    private IConnection clientConn;

    public NatsNode(RaaSOptions options) throws IOException {
        serviceConn = new Connection(options.getNatsOptions());
        if (options.isSingleSocket())
            clientConn = serviceConn;
        else
            clientConn = new Connection(options.getNatsOptions());
    }

    public NatsNode() throws IOException {
        this(new RaaSOptions());
    }

    @Override
    public Observable<ServiceInfo> register(String serviceName, Service service) {
        Subject<ServiceInfo> serviceInfoSubject = PublishSubject.create();
        ServiceInfo info = new ServiceInfo();
        return serviceConn
                .subscribeMsg(serviceName, "service")
                .flatMap(handshakeMsg -> onServiceConnected(service, handshakeMsg)
                        .doOnSubscribe(x -> {
                            info.calledNum++;
                            serviceInfoSubject.onNext(info);
                        })
                        .doOnComplete(() -> {
                            info.completedNum++;
                            serviceInfoSubject.onNext(info);
                        })
                        .doOnError(err -> {
                            info.errorNum++;
                            serviceInfoSubject.onNext(info);
                        })
                        .ofType(ServiceInfo.class)
                ).mergeWith(serviceInfoSubject);
    }

    @Override
    public Observable<byte[]> call(String serviceName, Observable<byte[]> outputData) {
        String clientPort = Utils.randomID();
        IConnection conn = clientConn;
        Subject<Boolean> onPingSubject = PublishSubject.create();
        Observable<byte[]> out = conn.request(serviceName, clientPort.getBytes(), 5, TimeUnit.SECONDS)
                .map(msg -> new String(msg.getBody()))
                .doOnSuccess(servicePort -> doOnPing(onPingSubject, conn, servicePort))
                .doOnSuccess(servicePort -> System.out.println("servicePort:" + servicePort))
                .flatMapObservable(servicePort -> outputData
                        .doOnNext(body -> outputNext(conn, servicePort, body))
                        .doOnComplete(() -> outputComplete(conn, servicePort))
                        .doOnError(err -> outputError(conn, servicePort, err))
                        .doOnDispose(() -> outputError(conn, servicePort, new Exception("dispose")))
                )
                .filter(x -> false);
        return observeInputData(conn, clientPort, onPingSubject)
                .timeout(2,TimeUnit.MINUTES)
                .mergeWith(out)
                .doFinally(onPingSubject::onComplete);
    }

    private void doOnPing(Observable<Boolean> onPingSubject, IConnection conn, String servicePort) {
        onPingSubject
                .doOnNext(x ->System.out.println("onPing"))
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
                .map(MSG::getBody)
                .map(DataOuterClass.Data::parseFrom)
                .takeUntil(data -> data.getTypeCase().getNumber() == 2)
                .flatMap(data -> Observable.create(emitter -> {
                    switch (data.getTypeCase().getNumber()) {
                        case 1:
                            emitter.onNext(data.getRaw().toByteArray());
                            break;
                        case 2:
                            String err = data.getFinal();
                            if (err != null && !err.isEmpty())
                                throw new Exception(err);
                        case 3:
                            pingPongSubject.onNext(true);
                            break;
                        case 4:
                            pingPongSubject.onNext(false);
                            break;
                    }
                    emitter.onComplete();
                }))
//                .doOnNext(data -> {
//                    if (data.getTypeCase().getNumber() == 2) {
//                        String err = data.getFinal();
//                        if (err != null && !err.isEmpty())
//                            throw new Exception(err);
//                    }
//                })
//                .takeWhile(x -> x.getTypeCase().getNumber() == 1)
//                .map(data -> data.getRaw().toByteArray())
                ;
    }

    //temporarily emit output data
    private Observable<byte[]> onServiceConnected(Service service, MSG handshakeMsg) {
        IConnection conn = serviceConn;
        String clientPort = new String(handshakeMsg.getBody());
        System.out.println("clientPort:" + clientPort);
        String servicePort = Utils.randomID();
        MSG replyMsg = new MSG(handshakeMsg.getReplyTo(), servicePort.getBytes());
        Subject<byte[]> inputSubject = PublishSubject.create();
        Subject<Boolean> onPongSubject = PublishSubject.create();
        Observable<byte[]> inputData = observeInputData(conn, servicePort, onPongSubject)
                .timeout(2,TimeUnit.MINUTES)
                .doOnNext(inputSubject::onNext)
                .doOnComplete(inputSubject::onComplete)
                .doOnError(inputSubject::onError)
                .doOnDispose(inputSubject::onComplete)
                .mergeWith(Observable.create(emitter -> {
                    conn.publish(replyMsg);
                    emitter.onComplete();
                }));
        return service.onCall(new NatsContext(inputSubject, this))
                .doOnNext(raw -> outputNext(conn, clientPort, raw))
                .doOnComplete(() -> outputComplete(conn, clientPort))
                .doOnError(err -> outputError(conn, clientPort, err))
                .mergeWith(inputData)
                .mergeWith(intervalPing(conn, clientPort, onPongSubject))
                .doFinally(onPongSubject::onComplete)
                .doFinally(inputSubject::onComplete)
                .doFinally(() -> System.out.println("call on final"))
                ;
    }

    private Observable<byte[]> intervalPing(IConnection conn, String clientPort, Observable<Boolean> onPongSubject) {
        Observable<Long> ping = Observable.interval(0, 1, TimeUnit.MILLISECONDS)
                .takeUntil(onPongSubject
                        .doOnNext(x ->System.out.println("onPong"))
                )
                .mergeWith(Observable.timer(0, TimeUnit.MILLISECONDS)
                        .doOnNext(x -> conn.publish(new MSG(clientPort, pingMessage)))
                )
                .takeLast(1)
                .timeout(5, TimeUnit.SECONDS);
        return Observable.interval(1, TimeUnit.MINUTES)
                .flatMap(x -> ping)
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
