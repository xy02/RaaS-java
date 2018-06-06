package com.github.xy02.raas.nats;

import java.nio.ByteBuffer;

import com.github.xy02.raas.ServiceContext;

import io.reactivex.Completable;
import io.reactivex.Observable;

public class NatsContext implements ServiceContext{

    private Observable<byte[]> inputData;

    public NatsContext(Observable<byte[]> inputData){
        this.inputData = inputData;
    }

    @Override
    public Observable<byte[]> getInputData() {
        return inputData;
    }

    @Override
    public Observable<byte[]> call(String serviceName, Observable<byte[]> outputData) {
        return null;
    }

    @Override
    public Observable<byte[]> subscribe(String subejct) {
        return null;
    }

    @Override
    public Completable publish(String subejct, byte[] data) {
        return null;
    }
}