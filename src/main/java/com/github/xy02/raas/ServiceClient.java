package com.github.xy02.raas;

import io.reactivex.Observable;

import java.io.IOException;

public interface ServiceClient {
    Observable<byte[]> call(String serviceName, Observable<byte[]> outputData);

    Observable<byte[]> subscribe(String subject);

    void publish(String subject, byte[] data) throws IOException;
}