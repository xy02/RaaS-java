package com.github.xy02.raas;

import java.nio.ByteBuffer;

import io.reactivex.Observable;

public interface ServiceContext extends ServiceClient {
    Observable<byte[]> getInputData();
}