package com.github.xy02.raas.nats;

import com.github.xy02.nats.Client;
import com.github.xy02.nats.Msg;
import com.github.xy02.raas.DataOuterClass.Data;
import com.github.xy02.raas.RaaSNode;
import com.github.xy02.raas.Service;
import com.github.xy02.raas.ServiceInfo;
import com.github.xy02.raas.Utils;
import com.google.protobuf.ByteString;
import io.reactivex.Completable;
import io.reactivex.Observable;
import io.reactivex.disposables.Disposable;
import io.reactivex.subjects.PublishSubject;
import io.reactivex.subjects.Subject;

public class NatsNode implements RaaSNode {
    private static byte[] finalCompleteMessage = Data.newBuilder().setFinal("").build().toByteArray();
    private Client nc;

    public NatsNode(String address)  {
        nc = new Client(address);
    }

    @Override
    public Completable connect() {
        return Completable.fromObservable(nc.connect());
    }

    @Override
    public Observable<byte[]> call(String serviceName, Observable<byte[]> outputData) {
        return null;
    }

    @Override
    public Observable<byte[]> subscribe(String subejct) {
        return null;
    }

    @Override
    public Completable publish(String subejct, byte[] data) {
        return null;
    }

    @Override
    public Observable<ServiceInfo> register(String serviceName, Service service) {
        Subject<ServiceInfo> serviceInfoSubject = PublishSubject.create();
        ServiceInfo info = new ServiceInfo();
        Disposable subD = nc.subscribeMsg(serviceName)
                .flatMapCompletable(msg -> {
                    String clientPort = new String(msg.getBody());
                    String servicePort = Utils.randomID();
                    msg.setBody(servicePort.getBytes());
                    Observable<byte[]> inputData = nc.subscribeMsg(servicePort)
                            .map(Msg::getBody)
                            .map(Data::parseFrom)
                            .takeWhile(x -> x.getTypeCase().getNumber() == 1)
                            .map(x -> x.getRaw().toByteArray());
                    return service.onCall(new NatsContext(inputData))
                            .doFinally(() -> System.out.println("call on final"))
                            .map(x -> Data.newBuilder().setRaw(ByteString.copyFrom(x)).build())
                            .map(x -> new Msg(clientPort, x.toByteArray()))
                            .doOnComplete(() -> outputComplete(clientPort))
                            .doOnError(err -> outputError(clientPort, err))
                            .flatMapCompletable(out -> nc.publish(out))
                            .mergeWith(nc.publish(msg))
                            .doOnSubscribe(d -> {
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
                            });
                }).subscribe(() -> {
                }, serviceInfoSubject::onError);
        return serviceInfoSubject
                .doOnDispose(subD::dispose);
    }

    private void outputComplete(String clientPort) {
        nc.publish(new Msg(clientPort, finalCompleteMessage))
                .subscribe(() -> {
                }, err -> err.printStackTrace());
    }

    private void outputError(String clientPort, Throwable t) {
        byte[] body = Data.newBuilder().setFinal(t.getMessage()).build().toByteArray();
        nc.publish(new Msg(clientPort, body))
                .subscribe(() -> {
                }, err -> err.printStackTrace());
    }

}