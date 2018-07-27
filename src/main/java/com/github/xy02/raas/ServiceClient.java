package com.github.xy02.raas;

import io.reactivex.Observable;

import java.io.IOException;

public interface ServiceClient {
    Observable<byte[]> callService(String serviceName, byte[] requestBin, Observable<byte[]> output);

    Observable<byte[]> subscribe(String subject);

    void publish(String subject, byte[] data) throws IOException;
}