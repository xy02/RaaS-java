package com.github.xy02.raas.nats;

import com.github.xy02.nats.Connection;
import com.github.xy02.nats.IConnection;
import com.github.xy02.nats.MSG;
import com.github.xy02.raas.*;
import com.google.protobuf.ByteString;
import io.reactivex.Observable;
import io.reactivex.ObservableEmitter;
import io.reactivex.Observer;
import io.reactivex.Single;
import io.reactivex.subjects.PublishSubject;
import io.reactivex.subjects.Subject;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static com.github.xy02.raas.DataOuterClass.Data.TypeCase.FINAL;

public class NatsNode implements RaaSNode {

    //service connection
    private IConnection serviceConn;
    //client connection
    private IConnection clientConn;

    private RaaSOptions options;

    private String serviceID;

    private Map<Long, ObservableEmitter<DataOuterClass.Data>> inputEmitters = new ConcurrentHashMap<>();
    private Map<Long, String> serviceIDMap = new ConcurrentHashMap<>();
    private Map<Long, String> serviceIDMap = new ConcurrentHashMap<>();

    private long sessionID;



    public NatsNode(RaaSOptions options) throws IOException {
        this.options = options;
        serviceConn = new Connection(options.getNatsOptions());
        clientConn = serviceConn;
        if (!options.isSingleSocket())
            clientConn = new Connection(options.getNatsOptions());
        //input data listener
        serviceID = "n." + Utils.randomID();
        clientID = "n." + Utils.randomID();
        clientConn.subscribeMsg(clientID)
//                .map(msg -> DataOuterClass.Data.parseFrom(msg.getBody()))
                .doOnNext(msg -> {
                    String serviceID = msg.getReplyTo();
                    DataOuterClass.Data data = DataOuterClass.Data.parseFrom(msg.getBody());
                    long sessionID = data.getSessionId();
                    if(serviceID != null && !serviceID.isEmpty()){

                    }
                    ObservableEmitter<DataOuterClass.Data> emitter = inputEmitters.get(sessionID);
                    if (emitter == null) {
                        return;
                    }
                    emitter.onNext(data);
                })
                .subscribe();
    }

    public NatsNode() throws IOException {
        this(new RaaSOptions());
    }

    @Override
    public Observable<byte[]> callService(String serviceName, byte[] outputBin) {
        IConnection conn = clientConn;
        long sid = plusSessionID();
        Subject<Boolean> onPingSubject = PublishSubject.create();
        onPingSubject
//                .doOnNext(x -> System.out.println("onPing"))
                .doOnNext(x -> {
                    String serviceNodeID = inputReplys.get(sid);

                    if (serviceNodeID == null)
                        return;
                    byte[] pongMessage = DataOuterClass.Data.newBuilder()
                            .setSessionId(sid)
                            .setPingPong(false)
                            .build().toByteArray();
                    System.out.println("onPing"+serviceNodeID);
                    conn.publish(new MSG(serviceNodeID, pongMessage));
                })
                .subscribe();
        //listen input data
        return Observable.<DataOuterClass.Data>create(emitter -> inputEmitters.put(sid, emitter))
                .timeout(options.getInputTimeout(), TimeUnit.SECONDS)
                .takeUntil(data -> data.getTypeCase() == FINAL)
                .flatMap(data -> observeInputData(data, onPingSubject))
                .mergeWith(
                        //send output data
                        Observable.create(emitter -> {
                            byte[] body = DataOuterClass.Data.newBuilder()
                                    .setNodeId(nodeID)
                                    .setSessionId(sid)
                                    .setBin(ByteString.copyFrom(outputBin))
                                    .build().toByteArray();
                            conn.publish(new MSG("s." + serviceName, body));
                            emitter.onComplete();
                        })
                )
                //clean
                .doFinally(()->{
                    onPingSubject.onComplete();
                    inputEmitters.remove(sid);
                    inputReplys.remove(sid);
                })
                ;
    }

    @Override
    public Observable<ServiceInfo> registerService(String serviceName, Service service) {
        Subject<ServiceInfo> serviceInfoSubject = PublishSubject.create();
        ServiceInfo info = new ServiceInfo();
        return serviceConn
                .subscribeMsg("s." + serviceName, "service")
                .map(msg -> DataOuterClass.Data.parseFrom(msg.getBody()))
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
                ).mergeWith(serviceInfoSubject)
                .doFinally(serviceInfoSubject::onComplete)
                ;
    }

//    @Override
//    public Observable<ServiceInfo> registerUnaryService(String serviceName, UnaryService service) {
//        IConnection conn = serviceConn;
//        Subject<ServiceInfo> serviceInfoSubject = PublishSubject.create();
//        ServiceInfo info = new ServiceInfo();
//        return serviceConn
//                .subscribeMsg("us." + serviceName, "service")
//                .doOnNext(msg -> {
//                    DataOuterClass.Data data = DataOuterClass.Data.parseFrom(msg.getBody());
//                    msg.setBody(data.getRaw().toByteArray());
//                    msg.setReplyTo(data.getReply());
//                })
//                .flatMapSingle(msg -> service.onCall(new NatsContext<>(msg.getBody(), this))
//                        .map(raw -> DataOuterClass.Data.newBuilder().setRaw(ByteString.copyFrom(raw)).build().toByteArray())
//                        .doOnError(err -> {
//                            info.errorNum++;
//                            serviceInfoSubject.onNext(info);
//                        })
//                        .onErrorReturn(t -> DataOuterClass.Data.newBuilder().setFinal(t.getMessage()).build().toByteArray())
//                        .doOnSuccess(result -> conn.publish(new MSG(msg.getReplyTo(), result)))
//                        .doOnSuccess(x -> {
//                            info.completedNum++;
//                            serviceInfoSubject.onNext(info);
//                        })
//                        .doOnSubscribe(x -> {
//                            info.calledNum++;
//                            serviceInfoSubject.onNext(info);
//                        })
//                        .onErrorReturnItem(new byte[]{})
//                )
//                .ofType(ServiceInfo.class)
//                .mergeWith(serviceInfoSubject)
//                .doFinally(serviceInfoSubject::onComplete)
//                ;
//    }
//
//    @Override
//    public Single<byte[]> unaryCall(String serviceName, byte[] outputData, long timeout, TimeUnit timeUnit) {
//        IConnection conn = clientConn;
//        String reply = myRequestPrefix + Utils.randomID();
//        return onResponseSubject
//                .filter(msg -> msg.getSubject().equals(reply))
//                .mergeWith(Observable.create(emitter -> {
//                            byte[] body = DataOuterClass.Data.newBuilder()
//                                    .setReply(reply)
//                                    .setRaw(ByteString.copyFrom(outputData))
//                                    .build()
//                                    .toByteArray();
//                            conn.publish(new MSG("us." + serviceName, body));
//                            emitter.onComplete();
//                        })
////                        .doOnComplete(() -> System.out.println(Thread.currentThread().getName()))
//                )
////                .doOnNext(x -> System.out.println(Thread.currentThread().getName()))
//                .take(1)
//                .singleOrError()
//                .timeout(timeout, timeUnit)
//                .map(MSG::getBody)
//                .map(DataOuterClass.Data::parseFrom)
//                .flatMap(data -> Single.create(emitter -> {
//                    switch (data.getTypeCase().getNumber()) {
//                        case 1:
//                            emitter.onSuccess(data.getRaw().toByteArray());
//                            break;
//                        case 2:
//                            emitter.tryOnError(new Exception(data.getFinal()));
//                            break;
//                        default:
//                            throw new Exception("wrong data type");
//                    }
//                }))
//                ;
//    }


    @Override
    public Observable<ServiceInfo> registerUnaryService(String serviceName, UnaryService service) {
        return null;
    }

    @Override
    public Single<byte[]> callUnaryService(String serviceName, byte[] outputBin, long timeout, TimeUnit timeUnit) {
        return null;
    }

    @Override
    public Observable<byte[]> subscribe(String subject) {
        return clientConn.subscribeMsg(subject)
                .map(MSG::getBody);
    }

    @Override
    public void publish(String subject, byte[] data) throws IOException {
        clientConn.publish(new MSG(subject, data));
    }

    private Observable<byte[]> observeInputData(DataOuterClass.Data data, Observer<Boolean> pingPongSubject) {
        return Observable.create(emitter -> {
            String reply = data.getNodeId();
            long sessionID = data.getSessionId();
            if (reply != null && !reply.isEmpty())
                inputReplys.put(sessionID, reply);
            switch (data.getTypeCase()) {
                case BIN:
                    emitter.onNext(data.getBin().toByteArray());
                    break;
                case FINAL:
                    String err = data.getFinal();
                    if (err != null && !err.isEmpty())
                        throw new Exception(err);
                case PING_PONG:
                    System.out.println("pingPong:"+data.getPingPong());
                    pingPongSubject.onNext(data.getPingPong());
                    break;
            }
            emitter.onComplete();
        });
    }

    //temporarily emit output data
    private Observable<byte[]> onServiceConnected(Service service, DataOuterClass.Data req) {
        IConnection conn = serviceConn;
        String clientNodeID = req.getNodeId();
        long sessionID = req.getSessionId();
        System.out.println(clientNodeID);
        Subject<Boolean> onPongSubject = PublishSubject.create();

        return service.onCall(req.getBin().toByteArray(),this)
                .doOnNext(bin -> outputNext(conn, clientNodeID, sessionID, bin))
                .doOnComplete(() -> outputComplete(conn, clientNodeID, sessionID))
                .doOnError(err -> outputError(conn, clientNodeID, sessionID, err))
                .doFinally(onPongSubject::onComplete)
                .onErrorResumeNext(Observable.empty())
                .mergeWith(intervalPing(conn, clientNodeID,sessionID, onPongSubject))
                .mergeWith(Observable.create(emitter -> {
                    //response node id
                    byte[] firstReply = DataOuterClass.Data.newBuilder()
                            .setNodeId(nodeID)
                            .setSessionId(sessionID)
                            .build().toByteArray();
                    MSG replyMsg = new MSG(clientNodeID, firstReply);
                    conn.publish(replyMsg);
                    emitter.onComplete();
                }))
//                .doFinally(() -> System.out.println("call on final"))
        ;
    }

    private Observable<byte[]> intervalPing(IConnection conn, String clientID, long sessionID, Observable<Boolean> onPongSubject) {
        Observable<Boolean> ping = onPongSubject
                .doOnNext(x -> System.out.println("onPong"))
                .take(1)
                .mergeWith(Observable.create(emitter -> {
                    byte[] pingMessage = DataOuterClass.Data.newBuilder()
                            .setSessionId(sessionID)
                            .setPingPong(true)
                            .build().toByteArray();
                    conn.publish(new MSG(clientID, pingMessage));
                    emitter.onComplete();
                }))
                .timeout(options.getPongTimeout(), TimeUnit.SECONDS);
        return Observable.interval(options.getPingInterval(), TimeUnit.SECONDS)
                .flatMap(x -> ping)
                .takeUntil(onPongSubject.filter(x -> false))
                .ofType(byte[].class);
    }



}
