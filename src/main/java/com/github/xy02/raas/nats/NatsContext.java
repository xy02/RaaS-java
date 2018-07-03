package com.github.xy02.raas.nats;

import com.github.xy02.raas.RaaSNode;
import com.github.xy02.raas.ServiceContext;
import io.reactivex.Observable;
import io.reactivex.Single;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class NatsContext<T> implements ServiceContext<T> {

    private T inputData;
    private RaaSNode node;

    public NatsContext(T inputData, RaaSNode node) {
        this.inputData = inputData;
        this.node = node;
    }

    @Override
    public T getInputData() {
        return inputData;
    }

    @Override
    public Single<byte[]> unaryCall(String serviceName, byte[] outputData, long timeout, TimeUnit timeUnit) {
        return node.unaryCall(serviceName, outputData, timeout, timeUnit);
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