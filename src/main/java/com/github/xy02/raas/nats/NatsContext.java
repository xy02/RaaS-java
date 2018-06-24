package com.github.xy02.raas.nats;

import com.github.xy02.raas.RaaSNode;
import com.github.xy02.raas.ServiceContext;
import io.reactivex.Observable;

import java.io.IOException;

public class NatsContext implements ServiceContext {

    private Observable<byte[]> inputData;
    private RaaSNode node;

    public NatsContext(Observable<byte[]> inputData, RaaSNode node) {
        this.inputData = inputData;
        this.node = node;
    }

    @Override
    public Observable<byte[]> getInputData() {
        return inputData;
    }

    @Override
    public Observable<byte[]> call(String serviceName, Observable<byte[]> outputData) {
        return node.call(serviceName, outputData);
    }

    @Override
    public Observable<byte[]> subscribe(String subject) {
        return node.subscribe(subject);
    }

    @Override
    public void publish(String subject, byte[] data) throws IOException {
        node.publish(subject, data);
    }
}