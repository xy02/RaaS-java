package com.github.xy02.raas;

import io.reactivex.Single;

public interface UnaryService {
    Single<byte[]> onCall(byte[] inputBin, ServiceContext context);
}
