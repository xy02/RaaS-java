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

class Client3 {

    private IConnection conn;

    private String clientID;

    private RaaSOptions options;

    private Map<String, ObservableEmitter<Data.ServerOutput>> emitterMap = new ConcurrentHashMap<>();

    private Map<String, String> serverIDMap = new ConcurrentHashMap<>();

    Client3(IConnection conn, RaaSOptions options) {
        this.conn = conn;
        this.options = options;
        //input data listener
        clientID = "c." + Utils.randomID();
        conn.subscribeMsg(clientID)
                .map(msg -> Data.ServerOutput.parseFrom(msg.getBody()))
                .doOnNext(data -> {
                    String sessionID = data.getSessionId();
                    ObservableEmitter<Data.ServerOutput> emitter = emitterMap.get(sessionID);
                    if (emitter == null) {
                        return;
                    }
                    emitter.onNext(data);
                })
                .subscribe();
    }

    Observable<byte[]> callService(String serviceName, byte[] requestBin, Observable<byte[]> output) {
        String sid = Utils.randomID();

        Subject<ByteString> onPingSubject = PublishSubject.create();

        //listen input data
        return Observable.<Data.ServerOutput>create(emitter -> emitterMap.put(sid, emitter))
//                .timeout(Observable.timer(options.getPongTimeout(), TimeUnit.SECONDS), x -> Observable.timer(options.getInputTimeout(), TimeUnit.SECONDS))
                .takeUntil(data -> data.getBodyCase() == Data.ServerOutput.BodyCase.END || data.getBodyCase() == Data.ServerOutput.BodyCase.ERR)
                .flatMap(data -> observeInputData(data, onPingSubject))
                .mergeWith(
                        //send output data
                        Observable.create(emitter -> {
                            byte[] body = Data.Request.newBuilder()
                                    .setSessionId(sid)
                                    .setClientId(clientID)
                                    .setBin(ByteString.copyFrom(requestBin))
                                    .build().toByteArray();
                            conn.publish(new MSG( serviceName, body));
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

    private Observable<byte[]> observeInputData(Data.ServerOutput data, Observer<ByteString> onPing) {
        return Observable.create(emitter -> {
            switch (data.getBodyCase()) {
                case SERVER_ID:
                    String serverID = data.getServerId();
                    if (serverID != null && !serverID.isEmpty())
                        serverIDMap.put(data.getSessionId(), serverID);
                    break;
                case BIN:
                    emitter.onNext(data.getBin().toByteArray());
                    break;
                case END:
                    onPing.onComplete();
                    ByteString buf = data.getEnd();
                    if(!buf.isEmpty())
                        emitter.onNext(buf.toByteArray());
                    emitter.onComplete();
                    break;
                case ERR:
                    onPing.onComplete();
                    String err = data.getErr();
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