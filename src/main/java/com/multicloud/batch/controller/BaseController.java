package com.multicloud.batch.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

import static com.multicloud.batch.constant.PropertiesConstant.PROJECT_NAME;
import static com.multicloud.batch.constant.PropertiesConstant.PROJECT_VERSION;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

@Slf4j
@Validated
@RequiredArgsConstructor
@RestController
public class BaseController {

    @GetMapping(value = "/info")
    public ResponseEntity<?> info() {

        Map<String, Object> data = new HashMap<>();
        data.put("projectName", PROJECT_NAME);
        data.put("projectVersion", PROJECT_VERSION);
        data.put("currentThread", Thread.currentThread().getName());

        return ResponseEntity.status(HttpStatus.OK).body(data);
    }

}
