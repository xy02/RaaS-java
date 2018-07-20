package com.github.xy02.raas.nats;

import com.github.xy02.nats.Connection;
import com.github.xy02.nats.IConnection;
import com.github.xy02.nats.MSG;
import com.github.xy02.raas.RaaSNode;
import com.github.xy02.raas.Service;
import com.github.xy02.raas.ServiceInfo;
import com.github.xy02.raas.UnaryService;
import io.reactivex.Observable;
import io.reactivex.Single;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class NatsNode implements RaaSNode {

    private Client client;

    private Server server;

    private RaaSOptions options;

    private IConnection clientConn;

    public NatsNode(RaaSOptions options) throws IOException {
        this.options = options;
        IConnection serverConn = new Connection(options.getNatsOptions());
        IConnection clientConn = serverConn;
        if (!options.isSingleSocket())
            clientConn = new Connection(options.getNatsOptions());
        client = new Client(clientConn, options);
        server = new Server(serverConn, this, options);

    }

    public NatsNode() throws IOException {
        this(new RaaSOptions());
    }

    @Override
    public Observable<byte[]> callService(String serviceName, byte[] outputBin) {
        return client.callService(serviceName, outputBin);
    }

    @Override
    public Observable<ServiceInfo> registerService(String serviceName, Service service) {
        return server.registerService(serviceName, service);
    }

    @Override
    public Observable<ServiceInfo> registerUnaryService(String serviceName, UnaryService service) {
        return server.registerUnaryService(serviceName, service);
    }

    @Override
    public Single<byte[]> callUnaryService(String serviceName, byte[] outputBin) {
        return client.callUnaryService(serviceName, outputBin);
    }

    @Override
    public Single<byte[]> callUnaryService(String serviceName, byte[] outputBin, long timeout, TimeUnit timeUnit) {
        return client.callUnaryService(serviceName, outputBin, timeout, timeUnit);
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

}
