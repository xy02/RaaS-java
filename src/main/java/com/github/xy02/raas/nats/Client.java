package com.github.xy02.raas.nats;

import com.github.xy02.nats.IConnection;
import com.github.xy02.nats.MSG;
import com.github.xy02.raas.Data;
import com.github.xy02.raas.Utils;
import com.google.protobuf.ByteString;
import io.reactivex.Observable;
import io.reactivex.ObservableEmitter;
import io.reactivex.disposables.Disposable;
import io.reactivex.schedulers.Schedulers;
import io.reactivex.subjects.PublishSubject;
import io.reactivex.subjects.ReplaySubject;
import io.reactivex.subjects.Subject;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class Client {

    private IConnection conn;

    private String clientID;

    private RaaSOptions options;

    private Map<String, Session> sessionMap = new ConcurrentHashMap<>();

    class Session {
        String sessionID;
        String serverID;
        long clientOutputSequence = 1;
        long serverOutputSequence = 1;
        Subject<byte[]> inputBinSubject = PublishSubject.create();
        //        Subject<Data.ClientOutput.Builder> outputSubject = PublishSubject.create();
        Subject<String> serverIdSubject = ReplaySubject.create();
        //        Subject<Object> inputTimeoutSubject = PublishSubject.create();
        //store input data which if out of sequence
        Map<Long, Data.ServerOutput> serverOutputTempMap = new ConcurrentHashMap<>();

        Session(String sessionID) {
            this.sessionID = sessionID;
        }

        long nextClientOutputSequence() {
            return ++clientOutputSequence;
        }

        long nextServerOutputSequence() {
            return ++serverOutputSequence;
        }

        void outputClientData(Data.ClientOutput.Builder builder) throws Exception {
            if (this.serverID != null) {
                byte[] buf = builder.setClientOutputSequence(clientOutputSequence)
                        .setSessionId(sessionID)
                        .build().toByteArray();
                conn.publish(new MSG(serverID, buf));
                nextClientOutputSequence();
            }
        }

        void handleServerOutput(Data.ServerOutput data) throws Exception {
            //case
//            System.out.println(data.getBodyCase());
            switch (data.getBodyCase()) {
                case SERVER_ID:
                    String serverId = data.getServerId();
                    if (serverId != null && !serverId.isEmpty()) {
                        this.serverID = serverId;
                        serverIdSubject.onNext(serverId);
                        serverIdSubject.onComplete();
                    }
                    break;
                case PING:
                    outputClientData(
                            Data.ClientOutput.newBuilder()
                                    .setPong(data.getPing())
                    );
                    break;
                case BIN:
                    inputBinSubject.onNext(data.getBin().toByteArray());
                    break;
                case END:
                    if (!data.getEnd().isEmpty()) {
                        inputBinSubject.onNext(data.getEnd().toByteArray());
                    }
                    inputBinSubject.onComplete();
                    break;
                case ERR:
                    inputBinSubject.onError(new Exception(data.getErr()));
                    break;
                default:
                    inputBinSubject.onError(new Exception("unrecognized body"));
                    clear();
            }
            handleNextServerOutput();
        }

        void handleNextServerOutput() throws Exception {
            long next = nextServerOutputSequence();
            Data.ServerOutput data = serverOutputTempMap.get(next);
            if (data != null)
                handleServerOutput(data);
        }

        void clear() {
            inputBinSubject.onComplete();
            serverIdSubject.onComplete();
            serverOutputTempMap.clear();
//            inputTimeoutSubject.onComplete();
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
                    String sessionID = data.getSessionId();
                    ObservableEmitter<byte[]> emitter = requestEmitters.get(sessionID);
                    if (emitter != null) {
//                        emitter.onNext(msg);
                        emitter.onComplete();
                    }
//                    String sessionID = data.getSessionId();
//                    Session session = sessionMap.get(sessionID);
//                    if (session == null) {
//                        return;
//                    }
//                    //reset timeout
//                    session.inputTimeoutSubject.onNext(1);
//                    //check sequence
//                    long ss = data.getServerOutputSequence();
//                    if (ss != session.serverOutputSequence) {
//                        session.serverOutputTempMap.put(ss, data);
//                        return;
//                    }
//                    session.handleServerOutput(data);
                })
                .subscribe(x -> {
                }, Throwable::printStackTrace);
    }


    //test
    private Map<String, ObservableEmitter<byte[]>> requestEmitters = new ConcurrentHashMap<>();
    private long requestID;

    private synchronized long plusRequestID() {
        return ++requestID;
    }
    Observable<byte[]> callService(String serviceName, byte[] requestBin, Observable<byte[]> output) {
//        String sid = Utils.randomID(); //sessionID
        long id = plusRequestID();
        String sid = clientID + id;
        return Observable.<byte[]>create(emitter -> {
            requestEmitters.put(sid, emitter);
        })

//                .take(1)
                .mergeWith(Observable.create(emitter -> {
                            byte[] req = Data.Request.newBuilder()
                                    .setSessionId(sid)
                                    .setClientId(clientID)
                                    .setBin(ByteString.copyFrom(requestBin))
                                    .build().toByteArray();
                            conn.publish(new MSG(serviceName, req));
                            emitter.onComplete();
                        })
                )
//                .singleOrError()
//                .timeout(timeout, timeUnit)
                .doFinally(() -> requestEmitters.remove(sid))
                ;
    }

    Observable<byte[]> callService2(String serviceName, byte[] requestBin, Observable<byte[]> output) {
        //new session
        String sid = Utils.randomID();
        Session session = new Session(sid);
//        //map session
        sessionMap.put(sid, session);
        //send output data
        Disposable outDis = session.serverIdSubject
                .flatMap(serverId -> output
                        .doOnNext(data -> session.outputClientData(Data.ClientOutput.newBuilder().setBin(ByteString.copyFrom(data))))
                )
                .subscribe();

        return session.inputBinSubject
                .doFinally(() -> {
                    sessionMap.remove(sid);
                    outDis.dispose();
                    session.clear();
                })
                //send request data
                .mergeWith(
                        Observable.<byte[]>create(emitter -> {
                            byte[] req = Data.Request.newBuilder()
                                    .setSessionId(sid)
                                    .setClientId(clientID)
                                    .setBin(ByteString.copyFrom(requestBin))
                                    .build().toByteArray();
                            conn.publish(new MSG(serviceName, req));
                            emitter.onComplete();
                        }).subscribeOn(Schedulers.io())
                )
//                .mergeWith(
//                        Observable.create(emitter -> {
//                            session.inputBinSubject.onComplete();
//                            emitter.onComplete();
//                        })
//                )
                ;
    }

}
