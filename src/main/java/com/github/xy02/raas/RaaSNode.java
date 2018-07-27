package com.github.xy02.raas;

import io.reactivex.Observable;

public interface RaaSNode extends ServiceClient {
    //注册服务
    Observable<ServiceInfo> registerService(String serviceName, Service service);
}