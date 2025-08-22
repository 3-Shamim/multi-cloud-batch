//package com.multicloud.batch.scheduler;
//
//import lombok.RequiredArgsConstructor;
//import lombok.extern.slf4j.Slf4j;
//import org.springframework.scheduling.annotation.Scheduled;
//import org.springframework.stereotype.Component;
//
///**
// * Created by IntelliJ IDEA.
// * User: Md. Shamim Molla
// * Email: shamim.molla@vivasoftltd.com
// */
//
//@Slf4j
//@Component
//@RequiredArgsConstructor
//public class TestScheduler {
//
//    @Scheduled(cron = "${batch_job.sample}")
//    public void runTestSchedule() {
//
//        log.info("Test scheduler is running.");
//
//    }
//
//}