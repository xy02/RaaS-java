package com.github.xy02.raas;

import io.reactivex.Observable;

public interface Service {
    Observable<byte[]> onCall(byte[] inputBin,ServiceContext context);
}