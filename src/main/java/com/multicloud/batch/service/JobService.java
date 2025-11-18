package com.multicloud.batch.service;

import com.multicloud.batch.dto.JobDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class JobService {

    private final JdbcTemplate jdbcTemplate;

    private final JobRegistry jobRegistry;
    private final JobLauncher jobLauncher;
    private final JobOperator jobOperator;
    private final JobExplorer jobExplorer;

    public List<JobDTO> getJobStatuses() {

        List<JobDTO> jobs = new ArrayList<>();

        for (String jobName : jobRegistry.getJobNames()) {
            JobDTO jobStatus = getLatestJobStatus(jobName);
            jobs.add(jobStatus);
        }

        return jobs;
    }

    private JobDTO getLatestJobStatus(String jobName) {

        JobInstance instance = jobExplorer.getLastJobInstance(jobName);

        if (instance != null) {

            JobExecution latestExecution = jobExplorer.getLastJobExecution(instance);

            if (latestExecution == null) {
                return new JobDTO(jobName, "UNKNOWN", false);
            }

            return new JobDTO(jobName, latestExecution.getStatus().name(), latestExecution.isRunning());
        }

        return new JobDTO(jobName, "NEVER_EXECUTED", false);
    }

    public void startJobAsync(String jobName, LocalDate startDate) throws Exception {

        Job job = jobRegistry.getJob(jobName);

        boolean isRunning = jobExplorer.findRunningJobExecutions(jobName).stream()
                .anyMatch(JobExecution::isRunning);

        if (isRunning) {
            throw new RuntimeException(
                    "Job is already running. Please stop it first before starting it again. Job name: %s".formatted(jobName)
            );
        }

        CompletableFuture.runAsync(() -> startJob(job, startDate));

        holdForAMoment();

    }

    private void startJob(Job job, LocalDate startDate) {

        try {

            JobParameters params;

            if (startDate != null) {
                params = new JobParametersBuilder()
                        .addLong("time", System.currentTimeMillis())
                        .addLocalDate("startDate", startDate)
                        .toJobParameters();
            } else {
                params = new JobParametersBuilder()
                        .addLong("time", System.currentTimeMillis())
                        .toJobParameters();
            }

            jobLauncher.run(job, params);

        } catch (Exception e) {
            log.error("Error on starting job: [{}]", job.getName(), e);
        }

    }

    public void stopJob(String jobName) throws Exception {

        List<JobInstance> instances = jobExplorer.getJobInstances(jobName, 0, 10);

        for (JobInstance instance : instances) {
            List<JobExecution> executions = jobExplorer.getJobExecutions(instance);
            for (JobExecution execution : executions) {
                if (execution.isRunning()) {
                    jobOperator.stop(execution.getId());
                }
            }
        }

    }

    public void restartJobAsync(String jobName) {
        CompletableFuture.runAsync(() -> restartJob(jobName));
        holdForAMoment();
    }

    private void restartJob(String jobName) {

        List<JobInstance> instances = jobExplorer.getJobInstances(jobName, 0, 10);

        for (JobInstance instance : instances) {
            List<JobExecution> executions = jobExplorer.getJobExecutions(instance);
            for (JobExecution exec : executions) {
                if (exec.getStatus() == BatchStatus.FAILED || exec.getStatus() == BatchStatus.STOPPED) {
                    try {
                        jobOperator.restart(exec.getId());
                    } catch (Exception e) {
                        log.error("Error on restarting job: [{}]", jobName, e);
                    }
                }
            }
        }

    }

    public boolean isJobTrulyRunning(String jobName) {

        List<JobInstance> instances = jobExplorer.findJobInstancesByJobName(jobName, 0, 1);

        if (!instances.isEmpty()) {

            List<JobExecution> executions = jobExplorer.getJobExecutions(instances.getFirst());

            for (JobExecution execution : executions) {

                if (execution.isRunning()) {
                    return true;
                }

            }

        }

        return false;
    }

    public boolean hasJobEverCompleted(String jobName) {

        String sql = """
                SELECT EXISTS(
                    SELECT 1 FROM BATCH_JOB_INSTANCE i
                        JOIN BATCH_JOB_EXECUTION e ON i.JOB_INSTANCE_ID = e.JOB_INSTANCE_ID
                        JOIN BATCH_JOB_EXECUTION_PARAMS ep ON e.JOB_EXECUTION_ID = ep.JOB_EXECUTION_ID
                    WHERE i.JOB_NAME = 'mergeAwsBillingDataJob'
                        AND e.STATUS = 'COMPLETED'
                        AND ep.PARAMETER_NAME = 'orgId'
                        AND PARAMETER_VALUE = 1
                );
                """;

        Boolean exists = jdbcTemplate.queryForObject(sql, Boolean.class, jobName);

        return exists != null && exists;
    }

    private static void holdForAMoment() {
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            log.error("Can not block the thread", e);
        }
    }

}