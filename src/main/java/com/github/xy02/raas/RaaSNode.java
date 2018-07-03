package com.github.xy02.raas;

import io.reactivex.Observable;

public interface RaaSNode extends ServiceClient {
    //注册长会话服务，返回服务当前状态
    Observable<ServiceInfo> registerService(String serviceName, Service service);
    //注册一元服务（只有一项请求和应答数据的服务），返回服务当前状态
    Observable<ServiceInfo> registerUnaryService(String serviceName, UnaryService service);
}