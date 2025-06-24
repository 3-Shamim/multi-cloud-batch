package com.multicloud.batch.dto;

import java.time.LocalDateTime;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

public interface JobExecutionView {

    String getJobName();

    Long getJobExecutionId();

    String getStatus();

    LocalDateTime getStartTime();

    LocalDateTime getEndTime();

    String getExitCode();

    String getExitMessage();

}