package com.github.xy02.raas.nats;

import com.github.xy02.nats.IConnection;
import com.github.xy02.nats.MSG;
import com.github.xy02.raas.*;
import com.google.protobuf.ByteString;
import io.reactivex.Observable;
import io.reactivex.ObservableEmitter;
import io.reactivex.subjects.PublishSubject;
import io.reactivex.subjects.Subject;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class Server {

    private IConnection conn;

    private String serverID;

    private RaaSOptions options;

    private ServiceContext context;

    private Map<Long, ObservableEmitter<Data.ClientOutput>> emitterMap = new ConcurrentHashMap<>();

    Server(IConnection conn, ServiceContext context, RaaSOptions options) {
        this.conn = conn;
        this.context = context;
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

    public Observable<ServiceInfo> registerUnaryService(String serviceName, UnaryService service) {
        Subject<ServiceInfo> serviceInfoSubject = PublishSubject.create();
        ServiceInfo info = new ServiceInfo();
        return conn.subscribeMsg("us." + serviceName, "service")
                .map(msg -> Data.Request.parseFrom(msg.getBody()))
                .flatMapSingle(request -> service.onCall(request.getBin().toByteArray(), context)
                        .doOnError(err -> {
                            info.errorNum++;
                            serviceInfoSubject.onNext(info);
                        })
                        .doOnError(err -> outputError(conn, request.getClientId(), request.getSessionId(), err))
                        .doOnSuccess(x -> {
                            info.completedNum++;
                            serviceInfoSubject.onNext(info);
                        })
                        .doOnSubscribe(x -> {
                            info.calledNum++;
                            serviceInfoSubject.onNext(info);
                        })
                        .doOnSuccess(bin -> outputNext(conn, request.getClientId(), request.getSessionId(), bin))
                )
                .ofType(ServiceInfo.class)
                .onErrorResumeNext(Observable.empty())
                .mergeWith(serviceInfoSubject)
                .doFinally(serviceInfoSubject::onComplete)
                ;
    }

    public Observable<ServiceInfo> registerService(String serviceName, Service service) {
        Subject<ServiceInfo> serviceInfoSubject = PublishSubject.create();
        ServiceInfo info = new ServiceInfo();
        return conn
                .subscribeMsg("rs." + serviceName, "service")
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
                ).mergeWith(serviceInfoSubject)
                .doFinally(serviceInfoSubject::onComplete)
                ;
    }

    //temporarily emit output data
    private Observable<byte[]> onServiceConnected(Service service, Data.Request request) {
        long sessionID = request.getSessionId();
        String clientID = request.getClientId();
//        System.out.println(clientID);
        Subject<String> onPongSubject = PublishSubject.create();

        return Observable.<Data.ClientOutput>create(emitter -> emitterMap.put(sessionID, emitter))
                .takeUntil(data -> data.getTypeCase() == Data.ClientOutput.TypeCase.CANCEL)
                .doOnNext(data -> {
                    if (data.getTypeCase() == Data.ClientOutput.TypeCase.PONG)
                        onPongSubject.onNext(data.getPong());
                })
                .ofType(byte[].class)
                .mergeWith(
                        service.onCall(request.getBin().toByteArray(), context)
                                .doOnNext(bin -> outputNext(conn, clientID, sessionID, bin))
                                .doOnComplete(() -> outputComplete(conn, clientID, sessionID))
                                .doOnError(err -> outputError(conn, clientID, sessionID, err))
                                .doFinally(onPongSubject::onComplete)
                                .onErrorResumeNext(Observable.empty())
                )
                .mergeWith(intervalPing(conn, clientID, sessionID, onPongSubject))
                .mergeWith(Observable.create(emitter -> {
                    //response server id
                    byte[] firstReply = Data.ServerOutput.newBuilder()
                            .setServerId(serverID)
                            .setSessionId(sessionID)
                            .build().toByteArray();
                    MSG replyMsg = new MSG(clientID, firstReply);
                    conn.publish(replyMsg);
                    emitter.onComplete();
                }))
//                .doFinally(() -> System.out.println("call on final"))
                ;
    }

    private Observable<byte[]> intervalPing(IConnection conn, String clientID, long sessionID, Observable<String> onPongSubject) {
        Observable<String> ping = onPongSubject
                .doOnNext(x -> System.out.println("onPong"))
                .take(1)
                .mergeWith(Observable.create(emitter -> {
                    byte[] pingMessage = Data.ServerOutput.newBuilder()
                            .setSessionId(sessionID)
                            .setPing("")
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

    private void outputNext(IConnection conn, String nodeID, long sessionID, byte[] bin) throws IOException {
        byte[] data = Data.ServerOutput.newBuilder()
                .setSessionId(sessionID)
                .setBin(ByteString.copyFrom(bin))
                .build().toByteArray();
        conn.publish(new MSG(nodeID, data));
    }

    private void outputComplete(IConnection conn, String nodeID, long sessionID) throws IOException {
        byte[] data = Data.ServerOutput.newBuilder()
                .setSessionId(sessionID)
                .setFinal("")
                .build().toByteArray();
        conn.publish(new MSG(nodeID, data));
    }

    private void outputError(IConnection conn, String nodeID, long sessionID, Throwable t) throws IOException {
        byte[] body = Data.ServerOutput.newBuilder()
                .setSessionId(sessionID)
                .setFinal(t.getMessage())
                .build().toByteArray();
        conn.publish(new MSG(nodeID, body));
    }
}
