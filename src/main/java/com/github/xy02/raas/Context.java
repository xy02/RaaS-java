package com.github.xy02.raas;


import io.reactivex.Observable;

public interface Context extends ServiceClient {

    byte[] getRequestBin();

    Observable<byte[]> getInputObservable();

}
