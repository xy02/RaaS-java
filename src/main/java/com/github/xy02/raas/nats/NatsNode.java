package com.github.xy02.raas.nats;

import com.github.xy02.nats.Connection;
import com.github.xy02.nats.MSG;
import com.github.xy02.nats.Options;
import com.github.xy02.raas.DataOuterClass.Data;
import com.github.xy02.raas.RaaSNode;
import com.github.xy02.raas.Service;
import com.github.xy02.raas.ServiceInfo;
import com.github.xy02.raas.Utils;
import com.google.protobuf.ByteString;
import io.reactivex.Observable;
import io.reactivex.disposables.Disposable;
import io.reactivex.subjects.PublishSubject;
import io.reactivex.subjects.Subject;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class NatsNode implements RaaSNode {
    private static byte[] finalCompleteMessage = Data.newBuilder().setFinal("").build().toByteArray();

    private Connection nc;
    private Connection ncPub;

    public NatsNode(Options options) throws IOException {
        nc = new Connection(options);
        ncPub = new Connection(options);
    }

    @Override
    public Observable<ServiceInfo> register(String serviceName, Service service) {
        Subject<ServiceInfo> serviceInfoSubject = PublishSubject.create();
        ServiceInfo info = new ServiceInfo();
        return serviceInfoSubject.mergeWith(nc
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
//                        .onErrorResumeNext(Observable.empty())
                        )

        );
    }

    @Override
    public Observable<byte[]> call(String serviceName, Observable<byte[]> outputData) {
        String clientPort = Utils.randomID();
        return observeInputData(clientPort)
                .mergeWith(nc.request(serviceName, clientPort.getBytes(), 5, TimeUnit.SECONDS)
                        .map(msg -> new String(msg.getBody()))
                        .doOnSuccess(servicePort -> System.out.println("servicePort:" + servicePort))
                        .flatMapObservable(servicePort -> outputData
                                .doOnNext(body -> outputNext(servicePort, body))
                                .doOnComplete(() -> outputComplete(servicePort))
                                .doOnError(err -> outputError(servicePort, err))
                                .doOnDispose(() -> outputError(servicePort, new Exception("dispose")))
                        )
                        .filter(x -> false)
                );
    }

    @Override
    public Observable<byte[]> subscribe(String subejct) {
        return nc.subscribeMsg(subejct).map(MSG::getBody);
    }

    @Override
    public void publish(String subejct, byte[] data) {
        ncPub.publish(new MSG(subejct, data));
    }

    private Observable<byte[]> observeInputData(String port) {
        return nc.subscribeMsg(port)
                .map(MSG::getBody)
                .map(Data::parseFrom)
                .takeUntil(x -> x.getTypeCase().getNumber() == 2)
                .doOnNext(data -> {
                    if (data.getTypeCase().getNumber() == 2) {
                        String err = data.getFinal();
                        if (err != null && !err.isEmpty())
                            throw new Exception(err);
                    }
                })
                .filter(data -> data.getTypeCase().getNumber() == 1)
                .map(data -> data.getRaw().toByteArray())
                ;
    }

    //temporarily emit output data
    private Observable<byte[]> onServiceConnected(Service service, MSG handshakeMsg) {
        String clientPort = new String(handshakeMsg.getBody());
        System.out.println("clientPort:" + clientPort);
        String servicePort = Utils.randomID();
        MSG replyMsg = new MSG(handshakeMsg.getReplyTo(), servicePort.getBytes());
        Subject<byte[]> inputData = PublishSubject.create();
        Disposable d = observeInputData(servicePort)
                .doOnNext(inputData::onNext)
                .doOnComplete(inputData::onComplete)
                .doOnError(inputData::onError)
                .doOnDispose(inputData::onComplete)
                .subscribe();
        return service.onCall(new NatsContext(inputData, this))
                .doOnNext(raw -> outputNext(clientPort, raw))
                .doOnComplete(() -> outputComplete(clientPort))
                .doOnError(err -> outputError(clientPort, err))
                .doOnSubscribe(x -> ncPub.publish(replyMsg))
                .doFinally(() -> System.out.println("call on final"))
                .doFinally(d::dispose)
                ;
    }

    private void outputNext(String port, byte[] raw) {
        Data data = Data.newBuilder().setRaw(ByteString.copyFrom(raw)).build();
        ncPub.publish(new MSG(port, data.toByteArray()));
    }

    private void outputComplete(String port) {
        ncPub.publish(new MSG(port, finalCompleteMessage));
    }

    private void outputError(String port, Throwable t) {
        byte[] body = Data.newBuilder().setFinal(t.getMessage()).build().toByteArray();
        ncPub.publish(new MSG(port, body));
    }

}