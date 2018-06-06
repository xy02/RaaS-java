package com.github.xy02.raas;

import io.reactivex.Observable;

public interface Service {
    Observable<byte[]> onCall(ServiceContext context);
}