package com.github.xy02.raas.nats;

import com.github.xy02.nats.IConnection;
import com.github.xy02.nats.MSG;
import com.github.xy02.raas.Data;
import com.github.xy02.raas.Utils;
import com.google.protobuf.ByteString;
import io.reactivex.Observable;
import io.reactivex.ObservableEmitter;
import io.reactivex.Observer;
import io.reactivex.Single;
import io.reactivex.subjects.PublishSubject;
import io.reactivex.subjects.Subject;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static com.github.xy02.raas.Data.ServerOutput.TypeCase.FINAL;

class Client {

    private long _sessionID;

    private synchronized long plusSessionID() {
        return ++_sessionID;
    }

    private IConnection conn;

    private String clientID;

    private RaaSOptions options;

    private Map<Long, ObservableEmitter<Data.ServerOutput>> emitterMap = new ConcurrentHashMap<>();

    private Map<Long, String> serverIDMap = new ConcurrentHashMap<>();

    Client(IConnection conn, RaaSOptions options) {
        this.conn = conn;
        this.options = options;
        //input data listener
        clientID = "c." + Utils.randomID();
        conn.subscribeMsg(clientID)
                .map(msg -> Data.ServerOutput.parseFrom(msg.getBody()))
                .doOnNext(data -> {
                    long sessionID = data.getSessionId();
                    ObservableEmitter<Data.ServerOutput> emitter = emitterMap.get(sessionID);
                    if (emitter == null) {
                        return;
                    }
                    emitter.onNext(data);
                })
                .subscribe();
    }

    Single<byte[]> callUnaryService(String serviceName, byte[] outputBin) {
        return callUnaryService(serviceName, outputBin, options.getPongTimeout(), TimeUnit.SECONDS);
    }

    Single<byte[]> callUnaryService(String serviceName, byte[] outputBin, long timeout, TimeUnit timeUnit) {
        long sid = plusSessionID();
        //listen input data
        return Observable.<Data.ServerOutput>create(emitter -> emitterMap.put(sid, emitter))
                .take(1)
                .flatMap(data -> observeInputData(data, null))
                .timeout(timeout, timeUnit)
                .mergeWith(
                        //send output data
                        Observable.create(emitter -> {
                            byte[] body = Data.Request.newBuilder()
                                    .setSessionId(sid)
                                    .setClientId(clientID)
                                    .setBin(ByteString.copyFrom(outputBin))
                                    .build().toByteArray();
                            conn.publish(new MSG("us." + serviceName, body));
                            emitter.onComplete();
                        })
                )
                .singleOrError()
                //clean
                .doFinally(() -> emitterMap.remove(sid))
                ;
    }

    Observable<byte[]> callService(String serviceName, byte[] outputBin) {
        long sid = plusSessionID();

        Subject<String> onPingSubject = PublishSubject.create();

        //listen input data
        return Observable.<Data.ServerOutput>create(emitter -> emitterMap.put(sid, emitter))
                .timeout(Observable.timer(options.getPongTimeout(), TimeUnit.SECONDS), x -> Observable.timer(options.getInputTimeout(), TimeUnit.SECONDS))
                .takeUntil(data -> data.getTypeCase() == FINAL)
                .flatMap(data -> observeInputData(data, onPingSubject))
                .mergeWith(
                        //send output data
                        Observable.create(emitter -> {
                            byte[] body = Data.Request.newBuilder()
                                    .setSessionId(sid)
                                    .setClientId(clientID)
                                    .setBin(ByteString.copyFrom(outputBin))
                                    .build().toByteArray();
                            conn.publish(new MSG("rs." + serviceName, body));
                            emitter.onComplete();
                        })
                )
                .mergeWith(
                        onPingSubject
//                                .timeout(options.getInputTimeout(), TimeUnit.SECONDS)
                                .doOnNext(x -> System.out.println("onPing"))
                                .doOnNext(ping -> {
                                    String serverID = serverIDMap.get(sid);
                                    if (serverID == null)
                                        return;
                                    byte[] pongMessage = Data.ClientOutput.newBuilder()
                                            .setSessionId(sid)
                                            .setPong(ping)
                                            .build().toByteArray();
                                    conn.publish(new MSG(serverID, pongMessage));
                                })
                                .ofType(byte[].class)
                )
                //clean
                .doFinally(() -> {
                    onPingSubject.onComplete();
                    emitterMap.remove(sid);
                    serverIDMap.remove(sid);
                })
                ;
    }

    private Observable<byte[]> observeInputData(Data.ServerOutput data, Observer<String> onPing) {
        return Observable.create(emitter -> {
            switch (data.getTypeCase()) {
                case SERVER_ID:
                    String serverID = data.getServerId();
                    if (serverID != null && !serverID.isEmpty())
                        serverIDMap.put(data.getSessionId(), serverID);
                    break;
                case BIN:
                    emitter.onNext(data.getBin().toByteArray());
                    break;
                case FINAL:
                    String err = data.getFinal();
                    if (err != null && !err.isEmpty())
                        throw new Exception(err);
                case PING:
                    onPing.onNext(data.getPing());
                    break;
            }
            emitter.onComplete();
        });
    }
}
