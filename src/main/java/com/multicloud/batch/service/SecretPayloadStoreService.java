package com.multicloud.batch.service;

import com.multicloud.batch.dao.aws.payload.SecretPayload;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

@Slf4j
@Service
public class SecretPayloadStoreService {

    private final Map<String, SecretPayload> SECRET_STORE = new ConcurrentHashMap<>();

    public void put(String key, SecretPayload value) {
        SECRET_STORE.put(key, value);
    }

    public SecretPayload get(String key) {
        return SECRET_STORE.get(key);
    }

}
