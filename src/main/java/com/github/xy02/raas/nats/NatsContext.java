package com.github.xy02.raas.nats;

import com.github.xy02.raas.ServiceContext;
import io.reactivex.Observable;

public class NatsContext implements ServiceContext {

    private Observable<byte[]> inputData;
    private NatsNode node;

    public NatsContext(Observable<byte[]> inputData, NatsNode node) {
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
    public Observable<byte[]> subscribe(String subejct) {
        return node.subscribe(subejct);
    }

    @Override
    public void publish(String subejct, byte[] data) {
        node.publish(subejct, data);
    }
}