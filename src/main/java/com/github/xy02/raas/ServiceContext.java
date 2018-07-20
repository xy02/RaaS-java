package com.github.xy02.raas;

import io.reactivex.Observable;
import io.reactivex.Single;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public interface ServiceContext {
    Observable<byte[]> callService(String serviceName, byte[] outputBin);

    Single<byte[]> callUnaryService(String serviceName, byte[] outputBin);

    Single<byte[]> callUnaryService(String serviceName, byte[] outputBin, long timeout, TimeUnit timeUnit);

    Observable<byte[]> subscribe(String subject);

    void publish(String subject, byte[] data) throws IOException;
}