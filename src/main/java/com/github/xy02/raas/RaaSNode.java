package com.github.xy02.raas;

import io.reactivex.Observable;

public interface RaaSNode extends ServiceClient {
    //注册服务，返回服务当前状态
    Observable<ServiceInfo> register(String serviceName, Service service);
}