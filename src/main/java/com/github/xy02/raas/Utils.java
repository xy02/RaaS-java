package com.github.xy02.raas;

import de.huxhorn.sulky.ulid.ULID;

public class Utils {
    private static ULID ulid = new ULID();

    public static String randomID() {
//        return UUID.randomUUID().toString();
        return ulid.nextULID();
    }
}