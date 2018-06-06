package com.github.xy02.raas;

import io.reactivex.Completable;
import io.reactivex.Observable;

public interface ServiceClient {
    Observable<byte[]> call(String serviceName, Observable<byte[]> outputData);
    Observable<byte[]> subscribe(String subejct);
    Completable publish(String subejct, byte[] data);
}