package com.github.xy02.raas.nats;

import com.github.xy02.nats.IConnection;
import com.github.xy02.nats.MSG;
import com.github.xy02.raas.*;
import com.google.protobuf.ByteString;
import io.reactivex.Observable;
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

    private final ByteString PING = ByteString.copyFrom(new byte[0]);

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
                    long ss = data.getClientOutputSequence();
                    if (ss != session.clientOutputSequence) {
                        session.clientOutputTempMap.put(ss, data);
                        return;
                    }
                    session.handleClientOutput(data);
                })
                .subscribe();
    }

    class Session implements Context {
        String sessionID;
        String clientId;
        volatile long clientOutputSequence = 1;
        volatile long serverOutputSequence = 1;
        Subject<byte[]> inputBinSubject = PublishSubject.create();
        Subject<ByteString> onPongSubject = PublishSubject.create();
        Observable<ByteString> intervalPing;
        byte[] requestBin;

        //store input data which if out of sequence
        Map<Long, Data.ClientOutput> clientOutputTempMap = new ConcurrentHashMap<>();

        Session(String sessionID, String clientId, byte[] reqBin, long pingInterval, long pongTimeout) {
            this.sessionID = sessionID;
            this.clientId = clientId;
            this.requestBin = reqBin;
            //interval ping
            Observable<ByteString> ping = onPongSubject
//                    .doOnNext(x -> System.out.println("onPong"))
                    .take(1)
                    .filter(x -> x.equals(PING))
                    .mergeWith(Observable.create(emitter -> {
                        outputServerData(Data.ServerOutput
                                .newBuilder()
                                .setPing(PING));
                        emitter.onComplete();
                    }))
                    .timeout(pongTimeout, TimeUnit.SECONDS);
            intervalPing = Observable.interval(pingInterval, pingInterval, TimeUnit.SECONDS)
                    .flatMap(x -> ping)
                    .takeUntil(onPongSubject.filter(x -> false));
        }

        void clear() {
            inputBinSubject.onComplete();
            onPongSubject.onComplete();
            clientOutputTempMap.clear();
        }

        private synchronized long nextClientOutputSequence() {
            return ++clientOutputSequence;
        }

        private synchronized long nextServerOutputSequence() {
            return ++serverOutputSequence;
        }

        void outputServerData(Data.ServerOutput.Builder builder) throws Exception {
            byte[] buf = builder.setServerOutputSequence(serverOutputSequence)
                    .setSessionId(sessionID)
                    .build().toByteArray();
            conn.publish(new MSG(clientId, buf));
            nextServerOutputSequence();
        }

        void handleClientOutput(Data.ClientOutput data) {
            //case
            switch (data.getBodyCase()) {
                case BIN:
                    inputBinSubject.onNext(data.getBin().toByteArray());
                    break;
                case PONG:
                    onPongSubject.onNext(data.getPong());
                    break;
                case END:
                    if (!data.getEnd().isEmpty()) {
                        inputBinSubject.onNext(data.getEnd().toByteArray());
                    }
                    clear();
                    break;
                case CANCEL:
                    inputBinSubject.onError(new Exception(data.getCancel()));
                    clear();
                    break;
                default:
                    inputBinSubject.onError(new Exception("unrecognized body"));
                    clear();
            }
            handleNextClientOutput();
        }

        void handleNextClientOutput() {
            long next = nextClientOutputSequence();
            Data.ClientOutput data = clientOutputTempMap.get(next);
            if (data != null)
                handleClientOutput(data);
        }

        @Override
        public byte[] getRequestBin() {
            return requestBin;
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
    }

    Observable<ServiceInfo> registerService(String serviceName, Service service) {
        Subject<ServiceInfo> serviceInfoSubject = PublishSubject.create();
        ServiceInfo info = new ServiceInfo();
        return conn
                .subscribeMsg(serviceName, "service")
                .map(msg -> Data.Request.parseFrom(msg.getBody()))
                .flatMap(data -> onServiceConnected(service, data)
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
                        .onErrorResumeNext(Observable.empty())
                        .ofType(ServiceInfo.class)
                )
                .mergeWith(serviceInfoSubject)
                .doFinally(serviceInfoSubject::onComplete)
                ;
    }

    private final ByteString EMPTY_END = ByteString.copyFrom(new byte[0]);

    private Observable<?> onServiceConnected(Service service, Data.Request request) {
        String sid = request.getSessionId();
        String clientID = request.getClientId();
        byte[] reqBin = request.getBin().toByteArray();
//        System.out.println(new String(reqBin));
        Session session = new Session(sid, clientID, reqBin, options.getPingInterval(), options.getPongTimeout());
        //map session
        sessionMap.put(sid, session);
        return service.onCall(session)
                .doOnNext(bin -> session.outputServerData(Data.ServerOutput.newBuilder().setBin(ByteString.copyFrom(bin))))
                .doOnComplete(() -> session.outputServerData(Data.ServerOutput.newBuilder().setEnd(EMPTY_END)))
                .doFinally(() -> {
                    sessionMap.remove(sid);
                    session.clear();
                })
                .mergeWith(Observable.create(emitter -> {
                            //send server id
                            session.outputServerData(Data.ServerOutput.newBuilder().setServerId(serverID));
                            emitter.onComplete();
                        })
                )
                .mergeWith(session.intervalPing.ofType(byte[].class))
                ;
    }

//    private Observable<byte[]> onServiceConnected2(Service service, Data.Request request) {
//        //new session
//        String sid = request.getSessionId();
//        String clientID = request.getClientId();
//        Session session = new Session(clientID, options.getPingInterval(), options.getPongTimeout());
//        Subject<Data.ServerOutput.Builder> outputSubject = session.outputSubject;
//        Subject<byte[]> inputBinSubject = session.inputBinSubject;
//        //map session
//        sessionMap.put(sid, session);
//        //listen output
//        outputSubject
////                .observeOn(Schedulers.io())
//                .doOnNext(new Consumer<Data.ServerOutput.Builder>() {
//                    private long _sequence = 0;
//
//                    private long plusSequence() {
//                        return ++_sequence;
//                    }
//
//                    @Override
//                    public void accept(Data.ServerOutput.Builder builder) throws Exception {
//                        long sequence = plusSequence();
//                        byte[] data = builder
//                                .setSessionId(sid)
//                                .setServerOutputSequence(sequence)
//                                .build().toByteArray();
//                        conn.publish(new MSG(clientID, data));
//                    }
//                })
//                .onErrorResumeNext(Observable.empty())
//                .subscribe();
//        //call service
//        Context ctx = new Context() {
//            @Override
//            public byte[] getRequestBin() {
//                return request.getBin().toByteArray();
//            }
//
//            @Override
//            public Observable<byte[]> getInputObservable() {
//                return inputBinSubject;
//            }
//
//
//            @Override
//            public Observable<byte[]> callService(String serviceName, byte[] requestBin, Observable<byte[]> output) {
//                return client.callService(serviceName, requestBin, output);
//            }
//
//            @Override
//            public Observable<byte[]> subscribe(String subject) {
//                return client.subscribe(subject);
//            }
//
//            @Override
//            public void publish(String subject, byte[] data) throws IOException {
//                client.publish(subject, data);
//            }
//        };
//
//        return service.onCall(ctx)
//                .doOnNext(bin -> outputSubject.onNext(Data.ServerOutput.newBuilder().setBin(ByteString.copyFrom(bin))))
//                .doOnComplete(() -> outputSubject.onNext(Data.ServerOutput.newBuilder().setEnd(EMPTY_END)))
//                .doOnError(err -> outputSubject.onNext(Data.ServerOutput.newBuilder().setErr(err.getMessage() == null ? err.getClass().getSimpleName() : err.getMessage())))
//                .doFinally(() -> {
//                    sessionMap.remove(sid);
//                    session.clear();
//                })
////                .mergeWith(Observable.create(emitter -> {
////                            //send server id
////                            outputSubject.onNext(Data.ServerOutput.newBuilder().setServerId(serverID));
////                            emitter.onComplete();
////                        })
////                )
////                .mergeWith(session.intervalPing.ofType(byte[].class))
//                ;
//    }

}
