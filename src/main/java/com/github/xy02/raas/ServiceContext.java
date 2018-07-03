package com.github.xy02.raas;

public interface ServiceContext<T> extends ServiceClient {
    T getInputData();
}