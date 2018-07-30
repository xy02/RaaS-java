package com.github.xy02.raas.nats;

import com.github.xy02.nats.IConnection;
import com.github.xy02.nats.MSG;
import com.github.xy02.raas.*;
import com.google.protobuf.ByteString;
import io.reactivex.Completable;
import io.reactivex.Observable;
import io.reactivex.Observer;
import io.reactivex.functions.Consumer;
import io.reactivex.schedulers.Schedulers;
import io.reactivex.subjects.PublishSubject;
import io.reactivex.subjects.Subject;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;


class Server {

    private IConnection conn;

    private String serverID;

    private RaaSOptions options;

    private ServiceClient client;

    private Map<String, Session> sessionMap = new ConcurrentHashMap<>();

    class Session {
        long clientOutputSequence = 0;
        Subject<byte[]> inputBinSubject = PublishSubject.create();
        Subject<Data.ServerOutput.Builder> outputSubject = PublishSubject.create();
        Subject<ByteString> onPongSubject = PublishSubject.create();
        String clientId;
        Observable<ByteString> intervalPing;

        private final ByteString PING = ByteString.copyFrom(new byte[0]);

        Session(String clientId, long pingInterval, long pongTimeout) {
            this.clientId = clientId;
            //interval ping
            Observable<ByteString> ping = onPongSubject
//                    .doOnNext(x -> System.out.println("onPong"))
                    .take(1)
                    .filter(x -> x.equals(PING))
                    .mergeWith(Observable.create(emitter -> {
                        outputSubject.onNext(
                                Data.ServerOutput
                                        .newBuilder()
                                        .setPing(PING)
                        );
                        emitter.onComplete();
                    }))
                    .timeout(pongTimeout, TimeUnit.SECONDS);
            intervalPing = Observable.interval(pingInterval, pingInterval, TimeUnit.SECONDS)
                    .flatMap(x -> ping)
                    .takeUntil(outputSubject.filter(x -> false));
        }

        synchronized long nextClientOutputSequence() {
            return ++clientOutputSequence;
        }

        void clear() {
            inputBinSubject.onComplete();
            outputSubject.onComplete();
        }
    }

    Server(IConnection conn, ServiceClient client, RaaSOptions options) {
        this.conn = conn;
        this.client = client;
        this.options = options;
        //input data listener
        serverID = "s." + Utils.randomID();
        conn.subscribeMsg(serverID)
                .map(msg -> Data.ClientOutput.parseFrom(msg.getBody()))
                .doOnNext(data -> {
                    String sessionID = data.getSessionId();
                    Session session = sessionMap.get(sessionID);
                    if (session == null) {
                        return;
                    }
                    //check sequence
                    long shouldBe = session.nextClientOutputSequence();
                    if (shouldBe != data.getClientOutputSequence()) {
                        session.inputBinSubject.onError(new Exception("bad client sequence"));
                        return;
                    }
                    //case
                    Observer<byte[]> inputBinSubject = session.inputBinSubject;
                    switch (data.getBodyCase()) {
                        case BIN:
                            inputBinSubject.onNext(data.getBin().toByteArray());
                            break;
                        case PONG:
                            session.onPongSubject.onNext(data.getPong());
                            break;
                        case END:
                            if (!data.getEnd().isEmpty()) {
                                inputBinSubject.onNext(data.getEnd().toByteArray());
                            }
                            session.clear();
                            return;
                        case CANCEL:
                            inputBinSubject.onError(new Exception(data.getCancel()));
                            session.clear();
                            return;
                        default:
                            inputBinSubject.onError(new Exception("unrecognized body"));
                            session.clear();
                    }
                })
                .subscribe();
    }

    Observable<ServiceInfo> registerService(String serviceName, Service service) {
        Subject<ServiceInfo> serviceInfoSubject = PublishSubject.create();
        ServiceInfo info = new ServiceInfo();
        return conn
                .subscribeMsg(serviceName, "service")
                .map(msg -> Data.Request.parseFrom(msg.getBody()))
                .flatMapCompletable(data -> onServiceConnected(service, data)
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

    private final ByteString EMPTY_END = ByteString.copyFrom(new byte[0]);

    private Completable onServiceConnected(Service service, Data.Request request) {
        //new session
        String sid = request.getSessionId();
        String clientID = request.getClientId();
        Session session = new Session(clientID, options.getPingInterval(), options.getPongTimeout());
        Subject<Data.ServerOutput.Builder> outputSubject = session.outputSubject;
        Subject<byte[]> inputBinSubject = session.inputBinSubject;
        //map session
        sessionMap.put(sid, session);
        //listen output
        outputSubject
                .observeOn(Schedulers.io())
                .doOnNext(new Consumer<Data.ServerOutput.Builder>() {
                    private long _sequence = 0;

                    private long plusSequence() {
                        return ++_sequence;
                    }

                    @Override
                    public void accept(Data.ServerOutput.Builder builder) throws Exception {
                        long sequence = plusSequence();
                        byte[] data = builder
                                .setSessionId(sid)
                                .setServerOutputSequence(sequence)
                                .build().toByteArray();
                        conn.publish(new MSG(clientID, data));
                    }
                })
                .onErrorResumeNext(Observable.empty())
                .subscribe();
        //call service
        Context ctx = new Context() {
            @Override
            public byte[] getRequestBin() {
                return request.getBin().toByteArray();
            }

            @Override
            public Observable<byte[]> getInputObservable() {
                return inputBinSubject;
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
        };

        return Completable.fromObservable(service.onCall(ctx)
                .doOnNext(bin -> outputSubject.onNext(Data.ServerOutput.newBuilder().setBin(ByteString.copyFrom(bin))))
                .doOnComplete(() -> outputSubject.onNext(Data.ServerOutput.newBuilder().setEnd(EMPTY_END)))
                .doOnError(err -> outputSubject.onNext(Data.ServerOutput.newBuilder().setErr(err.getMessage() == null ? err.getClass().getSimpleName() : err.getMessage())))
                .doFinally(() -> {
                    sessionMap.remove(sid);
                    session.clear();
                })
        )
                .mergeWith(Completable.create(emitter -> {
                            //send server id
                            outputSubject.onNext(Data.ServerOutput.newBuilder().setServerId(serverID));
                            emitter.onComplete();
                        })
                )
                .mergeWith(Completable.fromObservable(session.intervalPing))
                ;
    }

}
