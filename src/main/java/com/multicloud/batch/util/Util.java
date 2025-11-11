package com.multicloud.batch.util;

import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

@Slf4j
public class Util {

    public static boolean isValidEmail(String email) {
        String emailPattern = "^[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~-]+@((\\[[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}])|(([a-zA-Z\\-0-9]+\\.)+[a-zA-Z]{2,}))$";
        Pattern p = Pattern.compile(emailPattern);
        java.util.regex.Matcher m = p.matcher(email);
        return m.matches();
    }

    public static String compressValue(String value) throws IOException {

        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();

        try (GZIPOutputStream gzipStream = new GZIPOutputStream(byteStream)) {
            gzipStream.write(value.getBytes(StandardCharsets.UTF_8));
        }

        return Base64.getEncoder().encodeToString(byteStream.toByteArray());
    }

    public static String decompressValue(String compressed) throws IOException {

        byte[] bytes = Base64.getDecoder().decode(compressed);

        try (GZIPInputStream gis = new GZIPInputStream(new ByteArrayInputStream(bytes));
             ByteArrayOutputStream out = new ByteArrayOutputStream()) {

            byte[] buffer = new byte[1024];
            int len;
            while ((len = gis.read(buffer)) != -1) {
                out.write(buffer, 0, len);
            }

            return out.toString(StandardCharsets.UTF_8);
        }

    }

    public static String nullToEmpty(String value) {
        return value == null ? "" : value;
    }

}
