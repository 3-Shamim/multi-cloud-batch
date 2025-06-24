package com.multicloud.batch.service;

import lombok.RequiredArgsConstructor;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

@RequiredArgsConstructor
@Service
public class EncryptionService {

    private final PasswordEncoder encoder;

    public String encryptPassword(String password) {
        return encoder.encode(password);
    }

    public boolean matchPassword(String originalPassword, String encryptedPassword) {
        return encoder.matches(originalPassword, encryptedPassword);
    }

}
