package com.solace.samples.rest.features.serdes.util;

public class Util {
    private Util() {}

    public static String getEnv(String key, String defaultValue) {
        String value = System.getenv(key);
        if (value == null) {
            return defaultValue;
        }
        return value;
    }
}
