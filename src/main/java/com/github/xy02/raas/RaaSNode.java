package com.github.xy02.raas;

import io.reactivex.Completable;
import io.reactivex.Observable;

public interface RaaSNode extends ServiceClient {
    //连接MQ
    Completable connect();
    //注册服务，返回服务当前状态
    Observable<ServiceInfo> register(String serviceName, Service service);
}