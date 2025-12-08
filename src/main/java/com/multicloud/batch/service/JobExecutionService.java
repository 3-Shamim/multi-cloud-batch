package com.multicloud.batch.service;

import com.multicloud.batch.dto.JobExecutionDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional
public class JobExecutionService {

    private final JobExplorer jobExplorer;
    private final JdbcTemplate jdbcTemplate;

    public List<JobExecutionDTO> getAllByJobName(String jobName, LocalDate date) {

        String query = """
                        SELECT
                        i.JOB_NAME AS jobName,
                        e.JOB_EXECUTION_ID AS jobExecutionId,
                        e.STATUS AS status,
                        e.CREATE_TIME AS createTime,
                        e.START_TIME AS startTime,
                        e.END_TIME AS endTime,
                        e.EXIT_CODE AS exitCode,
                        e.EXIT_MESSAGE AS exitMessage
                    FROM
                        BATCH_JOB_EXECUTION e
                    JOIN
                        BATCH_JOB_INSTANCE i ON e.JOB_INSTANCE_ID = i.JOB_INSTANCE_ID
                    WHERE i.JOB_NAME = ? AND e.CREATE_TIME BETWEEN ? AND ?
                    ORDER BY
                        e.CREATE_TIME DESC
                """;

        LocalDateTime from = date.atTime(0, 0, 0);
        LocalDateTime to = date.atTime(23, 59, 59);

        return jdbcTemplate.query(
                query,
                ps -> {
                    ps.setString(1, jobName);
                    ps.setObject(2, from);
                    ps.setObject(3, to);
                },
                (rs, rowNum) -> {

                    String exitMessage = rs.getString("exitMessage");

                    return new JobExecutionDTO(
                            rs.getString("jobName"),
                            rs.getLong("jobExecutionId"),
                            rs.getString("status"),
                            rs.getObject("createTime", LocalDateTime.class),
                            rs.getObject("startTime", LocalDateTime.class),
                            rs.getObject("endTime", LocalDateTime.class),
                            rs.getString("exitCode"),
                            StringUtils.hasText(exitMessage) ? exitMessage : null
                    );
                }
        );
    }

    public List<JobExecutionDTO> getExecutionsByJobName(String jobName, LocalDate date) {

        List<JobInstance> instances = jobExplorer.getJobInstances(jobName, 0, 100);

        List<JobExecutionDTO> result = new ArrayList<>();

        LocalDateTime from = date.atTime(0, 0, 0);
        LocalDateTime to = date.atTime(23, 59, 59);

        for (JobInstance instance : instances) {

            List<JobExecution> executions = jobExplorer.getJobExecutions(instance);

            List<JobExecutionDTO> executionDTOS = executions.stream()
                    .filter(exec -> !from.isAfter(exec.getCreateTime()) && !to.isBefore(exec.getCreateTime()))
                    .map(exec -> new JobExecutionDTO(
                            jobName,
                            exec.getId(),
                            exec.getStatus().name(),
                            exec.getCreateTime(),
                            exec.getStartTime(),
                            exec.getEndTime(),
                            exec.getExitStatus().getExitCode(),
                            StringUtils.hasText(exec.getExitStatus().getExitDescription())
                                    ? exec.getExitStatus().getExitDescription()
                                    : null
                    ))
                    .toList();

            result.addAll(executionDTOS);
        }

        return result;
    }

}
