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

import java.io.IOException;

public class NatsNode implements RaaSNode {
    private static byte[] finalCompleteMessage = Data.newBuilder().setFinal("").build().toByteArray();
    private Client nc;

    public NatsNode(String address) throws IOException {
        nc = new Client(address);
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
                    System.out.println("clientPort:"+clientPort);
                    String servicePort = Utils.randomID();
                    Msg replyMsg = new Msg(msg.getReplyTo(),servicePort.getBytes());
                    Subject<byte[]> inputData = PublishSubject.create();
                    Disposable d= nc.subscribeMsg(servicePort)
                            .map(Msg::getBody)
                            .map(Data::parseFrom)
                            .takeUntil(x -> x.getTypeCase().getNumber() == 2)
                            .doOnNext(data->{
                                int type = data.getTypeCase().getNumber();
                                if(type == 1)
                                    inputData.onNext(data.getRaw().toByteArray());
                                if(type == 2) {
                                    String err = data.getFinal();
                                    if(err==null || err.isEmpty())
                                        inputData.onComplete();
                                    else
                                        inputData.onError(new Exception(err));
                                }
                            })
                            .doOnDispose(inputData::onComplete)
                            .subscribe();
                    return service.onCall(new NatsContext(inputData))
                            .doOnComplete(() -> outputComplete(clientPort))
                            .doOnError(err -> outputError(clientPort, err))
                            .doFinally(() -> System.out.println("call on final"))
                            .map(x -> Data.newBuilder().setRaw(ByteString.copyFrom(x)).build())
                            .map(x -> new Msg(clientPort, x.toByteArray()))
                            .flatMapCompletable(out -> nc.publish(out))
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
                            .mergeWith(nc.publish(replyMsg))
                            .doOnTerminate(d::dispose);
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