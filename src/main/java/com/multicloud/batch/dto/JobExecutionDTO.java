package com.multicloud.batch.dto;

import java.time.LocalDateTime;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

public record JobExecutionDTO(String jobName,
                              long jobExecutionId,
                              String status,
                              LocalDateTime createTime,
                              LocalDateTime startTime,
                              LocalDateTime endTime,
                              String exitCode,
                              String exitMessage) {
}
