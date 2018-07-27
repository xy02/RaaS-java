package com.github.xy02.raas.nats;

import com.github.xy02.nats.IConnection;
import com.github.xy02.nats.MSG;
import com.github.xy02.raas.*;
import com.google.protobuf.ByteString;
import io.reactivex.Completable;
import io.reactivex.Observable;
import io.reactivex.ObservableEmitter;
import io.reactivex.Observer;
import io.reactivex.functions.Consumer;
import io.reactivex.schedulers.Schedulers;
import io.reactivex.subjects.PublishSubject;
import io.reactivex.subjects.Subject;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static com.github.xy02.raas.Data.ClientOutput.BodyCase.CANCEL;
import static com.github.xy02.raas.Data.ClientOutput.BodyCase.END;

public class Server {

    private IConnection conn;

    private String serverID;

    private RaaSOptions options;

    private ServiceClient client;

    private Map<Long, ObservableEmitter<Data.ClientOutput>> emitterMap = new ConcurrentHashMap<>();
    private Map<Long, Long> clientOutputSequenceMap = new ConcurrentHashMap<>();

    Server(IConnection conn, ServiceClient client, RaaSOptions options) {
        this.conn = conn;
        this.client = client;
        this.options = options;
        //input data listener
        serverID = "s." + Utils.randomID();
        conn.subscribeMsg(serverID)
                .map(msg -> Data.ClientOutput.parseFrom(msg.getBody()))
                .doOnNext(data -> {
                    long sessionID = data.getSessionId();
                    ObservableEmitter<Data.ClientOutput> emitter = emitterMap.get(sessionID);
                    if (emitter == null) {
                        return;
                    }
                    emitter.onNext(data);
                })
                .subscribe();
    }

    public Observable<ServiceInfo> registerService(String serviceName, Service service) {
        Subject<ServiceInfo> serviceInfoSubject = PublishSubject.create();
        ServiceInfo info = new ServiceInfo();
        return conn
                .subscribeMsg(serviceName, "service")
                .map(msg -> Data.Request.parseFrom(msg.getBody()))
                .flatMapCompletable(data -> Completable.fromObservable(onServiceConnected(service, data))
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
                        .onErrorComplete()
                )
                .<ServiceInfo>toObservable()
                .mergeWith(serviceInfoSubject)
                .doFinally(serviceInfoSubject::onComplete)
                ;
    }

    private Observable<byte[]> onServiceConnected(Service service, Data.Request request) {
        long sessionID = request.getSessionId();
        String clientID = request.getClientId();

        Subject<Data.ServerOutput.Builder> outputSubject = PublishSubject.create();
        Subject<ByteString> onPongSubject = PublishSubject.create();
        Subject<byte[]> clientInputSubject = PublishSubject.create();

        return outputSubject
                .observeOn(Schedulers.io())
                .doOnNext(new Consumer<Data.ServerOutput.Builder>() {
                    private long _sequence = 0;

                    private synchronized long plusSequence() {
                        return ++_sequence;
                    }

                    @Override
                    public void accept(Data.ServerOutput.Builder builder) throws Exception {
                        long sequence = plusSequence();
                        System.out.println("sessionID:" + sessionID + " s:" + sequence + " tid" + Thread.currentThread().getId());
                        byte[] data = builder.setSessionId(sessionID)
                                .setServerOutputSequence(sequence)
                                .build().toByteArray();
                        conn.publish(new MSG(clientID, data));
                    }
                })
                .ofType(byte[].class)
                //call service
                .mergeWith(
                        service.onCall(new Context() {
                            @Override
                            public byte[] getRequestBin() {
                                return request.getBin().toByteArray();
                            }

                            @Override
                            public Observable<byte[]> getInputObservable() {
                                return clientInputSubject;
                            }


                            @Override
                            public Observable<byte[]> callService(String serviceName, byte[] requestBin, Observable<byte[]> output) {
                                return client.callService(serviceName, requestBin, output);
                            }

                            @Override
                            public Observable<byte[]> subscribe(String subject) {
                                return client.subscribe(subject);
                            }

                            @Override
                            public void publish(String subject, byte[] data) throws IOException {
                                client.publish(subject, data);
                            }
                        })
                                .doOnNext(bin -> outputSubject.onNext(Data.ServerOutput.newBuilder().setBin(ByteString.copyFrom(bin))))
                                .doOnComplete(() -> outputSubject.onNext(Data.ServerOutput.newBuilder().setEnd(null)))
                                .doOnError(err -> outputSubject.onNext(Data.ServerOutput.newBuilder().setErr(err.getMessage() == null ? err.getClass().getSimpleName() : err.getMessage())))
                                .doFinally(() -> {
                                    outputSubject.onComplete();
                                    onPongSubject.onComplete();
                                    clientInputSubject.onComplete();
                                    clientOutputSequenceMap.remove(sessionID);
                                    emitterMap.remove(sessionID);
                                })
                                .onErrorResumeNext(Observable.empty())
                )
                //listen input
                .mergeWith(
                        Observable.<Data.ClientOutput>create(emitter -> emitterMap.put(sessionID, emitter))
                                .takeUntil(data -> data.getBodyCase() == END || data.getBodyCase() == CANCEL)
                                //get input data
                                .flatMap(data -> observeInputData(data, clientInputSubject, onPongSubject))
                )
                //interval ping
                .mergeWith(intervalPing(clientID, sessionID, onPongSubject, outputSubject))
                //send server id
                .mergeWith(Observable.create(emitter -> {
                    outputSubject.onNext(Data.ServerOutput.newBuilder().setServerId(serverID));
                    emitter.onComplete();
                }))
                ;
    }

    private Observable<byte[]> observeInputData(Data.ClientOutput data, Subject<byte[]> clientInputSubject, Observer<ByteString> onPongSubject) {
        return Observable.create(emitter -> {
            long sid = data.getSessionId();
            //check sequence
            long shouldbe = clientOutputSequenceMap.get(sid) + 1;
            if (shouldbe != data.getClientOutputSequence()) {
                emitter.tryOnError(new Exception("bad client sequence"));
                return;
            }
            clientOutputSequenceMap.put(sid, shouldbe);
            switch (data.getBodyCase()) {
                case BIN:
                    clientInputSubject.onNext(data.getBin().toByteArray());
                    break;
                case PONG:
                    onPongSubject.onNext(data.getPong());
                    break;
                case END:
                    if (!data.getEnd().isEmpty()) {
                        clientInputSubject.onNext(data.getEnd().toByteArray());
                        clientInputSubject.onComplete();
                    }
                    break;
                case CANCEL:
                    emitter.tryOnError(new Exception(data.getCancel()));
                    return;
                default:
                    emitter.tryOnError(new Exception("unrecognized body"));
                    return;
            }
            emitter.onComplete();
        });
    }

    private final static ByteString PING = ByteString.copyFrom(new byte[0]);

    private Observable<byte[]> intervalPing(String clientID, long sessionID, Observable<ByteString> onPongSubject, Subject<Data.ServerOutput.Builder> outputSubject) {
        Observable<ByteString> ping = onPongSubject
                .doOnNext(x -> System.out.println("onPong"))
                .take(1)
                .filter(x -> x.equals(PING))
                .mergeWith(Observable.create(emitter -> {
                    outputSubject.onNext(Data.ServerOutput.newBuilder()
                            .setPing(PING)
                    );
                    emitter.onComplete();
                }))
                .timeout(options.getPongTimeout(), TimeUnit.SECONDS);
        return Observable.interval(options.getPingInterval(), TimeUnit.SECONDS)
                .flatMap(x -> ping)
                .takeUntil(onPongSubject.filter(x -> false))
                .ofType(byte[].class);
    }

}
