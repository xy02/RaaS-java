package com.github.xy02.raas.nats;

import com.github.xy02.nats.IConnection;
import com.github.xy02.nats.MSG;
import com.github.xy02.raas.Data;
import com.github.xy02.raas.Utils;
import com.google.protobuf.ByteString;
import io.reactivex.Observable;
import io.reactivex.Observer;
import io.reactivex.disposables.Disposable;
import io.reactivex.functions.Consumer;
import io.reactivex.schedulers.Schedulers;
import io.reactivex.subjects.PublishSubject;
import io.reactivex.subjects.ReplaySubject;
import io.reactivex.subjects.Subject;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

class Client {

    private long _sessionID;

    private synchronized long plusSessionID() {
        return ++_sessionID;
    }

    private IConnection conn;

    private String clientID;

    private RaaSOptions options;

    private Map<Long, Session> sessionMap = new ConcurrentHashMap<>();

    class Session {
        Observer<byte[]> inputBinSubject;
        Observer<Data.ClientOutput.Builder> outputSubject;
        Observer<String> serverIdSubject;
        long serverOutputSequence = 0;
        Subject<Object> inputTimeoutSubject;

        Session(Observer<byte[]> inputBinSubject, Observer<Data.ClientOutput.Builder> outputSubject, Observer<String> serverIdSubject) {
            this.inputBinSubject = inputBinSubject;
            this.outputSubject = outputSubject;
            this.serverIdSubject = serverIdSubject;
            inputTimeoutSubject
                    .timeout(options.getInputTimeout(), TimeUnit.SECONDS)
                    .subscribe(x -> {
                    }, inputBinSubject::onError);
        }

        synchronized long nextServerOutputSequence() {
            return ++serverOutputSequence;
        }

        void clear() {
            inputBinSubject.onComplete();
            outputSubject.onComplete();
            serverIdSubject.onComplete();
            inputTimeoutSubject.onComplete();
        }
    }

    Client(IConnection conn, RaaSOptions options) {
        this.conn = conn;
        this.options = options;
        //input data listener
        clientID = "c." + Utils.randomID();
        conn.subscribeMsg(clientID)
                .map(msg -> Data.ServerOutput.parseFrom(msg.getBody()))
                .doOnNext(data -> {
                    long sessionID = data.getSessionId();
                    Session session = sessionMap.get(sessionID);
                    if (session == null) {
                        return;
                    }
                    //check sequence
                    long shouldBe = session.nextServerOutputSequence();
                    if (shouldBe != data.getServerOutputSequence()) {
                        session.inputBinSubject.onError(new Exception("bad server sequence"));
                        return;
                    }
                    //reset timeout
                    session.inputTimeoutSubject.onNext(1);
                    //case
                    Observer<byte[]> inputBinSubject = session.inputBinSubject;
                    switch (data.getBodyCase()) {
                        case SERVER_ID:
                            String serverId = data.getServerId();
                            if (serverId != null && !serverId.isEmpty()) {
                                session.serverIdSubject.onNext(serverId);
                                session.serverIdSubject.onComplete();
                            }
                            break;
                        case PING:
                            session.outputSubject.onNext(
                                    Data.ClientOutput.newBuilder()
                                            .setPong(data.getPing())
                            );
                            break;
                        case BIN:
                            inputBinSubject.onNext(data.getBin().toByteArray());
                            break;
                        case END:
                            if (data.getEnd().isEmpty())
                                break;
                            inputBinSubject.onNext(data.getEnd().toByteArray());
                            session.clear();
                            break;
                        case ERR:
                            inputBinSubject.onError(new Exception(data.getErr()));
                            session.clear();
                            return;
                        default:
                            inputBinSubject.onError(new Exception("unrecognized body"));
                    }
                })
                .subscribe(x -> {
                }, Throwable::printStackTrace);
    }

    Observable<byte[]> callService(String serviceName, byte[] requestBin, Observable<byte[]> output) {
        //new session
        long sid = plusSessionID();
        Subject<byte[]> inputBinSubject = PublishSubject.create();
        Subject<Data.ClientOutput.Builder> outputSubject = PublishSubject.create();
        Subject<String> serverIdSubject = ReplaySubject.create();
        Session session = new Session(inputBinSubject, outputSubject, serverIdSubject);
        //map session
        sessionMap.put(sid, session);
        //send request data
        byte[] req = Data.Request.newBuilder()
                .setSessionId(sid)
                .setClientId(clientID)
                .setBin(ByteString.copyFrom(requestBin))
                .build().toByteArray();
        try {
            conn.publish(new MSG(serviceName, req));
        } catch (IOException e) {
            inputBinSubject.onError(e);
        }
        //listen output
        outputSubject
                .observeOn(Schedulers.io())
                .doOnNext(new Consumer<Data.ClientOutput.Builder>() {
                    private long _sequence = 0;

                    private long plusSequence() {
                        return ++_sequence;
                    }

                    @Override
                    public void accept(Data.ClientOutput.Builder builder) throws Exception {
                        long sequence = plusSequence();
                        byte[] data = builder
                                .setSessionId(sid)
                                .setClientOutputSequence(sequence)
                                .build().toByteArray();
                        conn.publish(new MSG(serviceName, data));
                    }
                })
                .subscribe();
        //send output data
        Disposable outDis = serverIdSubject
                .flatMap(serverID -> {
                    if (output == null) {
                        return Observable.empty();
                    }
                    return output;
                })
                .doOnNext(data -> outputSubject.onNext(Data.ClientOutput.newBuilder().setBin(ByteString.copyFrom(data))))
                .subscribe();
        //return
        return inputBinSubject
                .doFinally(() -> {
                    sessionMap.remove(sid);
                    session.clear();
                    outDis.dispose();
                });
    }

}
