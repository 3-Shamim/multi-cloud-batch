package com.multicloud.batch.controller;

import com.multicloud.batch.dto.JobDTO;
import com.multicloud.batch.dto.JobExecutionDTO;
import com.multicloud.batch.service.JobExecutionService;
import com.multicloud.batch.service.JobService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

@Slf4j
@Validated
@RequiredArgsConstructor
@Controller
public class UIController {

    private final JobService jobService;
    private final JobExecutionService jobExecutionService;

    @GetMapping
    public String home(@RequestParam(required = false) String jobName,
                       @RequestParam(required = false) String msg,
                       Model model) {

        List<JobDTO> jobs = jobService.getJobStatuses();

        model.addAttribute("jobName", jobName);
        model.addAttribute("msg", msg);
        model.addAttribute("jobs", jobs);

        model.addAttribute("title", "Home | Jobs");

        return "home";
    }

    @GetMapping("/jobs/{jobName}")
    public String jobDetails(@PathVariable String jobName,
                             @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate date,
                             Model model) {

        if (date == null) {
            date = LocalDate.now();
        }

        List<JobExecutionDTO> executions = jobExecutionService.getAllByJobName(jobName, date);
        model.addAttribute("jobName", jobName);
        model.addAttribute("executions", executions);
        model.addAttribute("date", date);

        return "job-details";
    }

    @GetMapping("/jobs/start/{jobName}")
    public String startJob(@PathVariable String jobName,
                           @RequestParam(value = "startDate", required = false)
                           @DateTimeFormat(iso = DateTimeFormat.ISO.DATE)
                           LocalDate startDate) {

        try {
            jobService.startJobAsync(jobName, startDate);
            String msg = "successfully started";
            String encodedMsg = URLEncoder.encode(msg, StandardCharsets.UTF_8);
            return "redirect:/?jobName=%s&msg=%s".formatted(jobName, encodedMsg);
        } catch (Exception e) {
            String msg = "can't start due to [%s]".formatted(e.getMessage());
            String encodedMsg = URLEncoder.encode(msg, StandardCharsets.UTF_8);
            return "redirect:/?jobName=%s&msg=%s".formatted(jobName, encodedMsg);
        }

    }

    @GetMapping("/jobs/stop/{jobName}")
    public String stopJob(@PathVariable String jobName) {

        try {
            jobService.stopJob(jobName);
            String msg = "successfully stopped";
            String encodedMsg = URLEncoder.encode(msg, StandardCharsets.UTF_8);
            return "redirect:/?jobName=%s&msg=%s".formatted(jobName, encodedMsg);
        } catch (Exception e) {
            String msg = "can't stop due to [%s]".formatted(e.getMessage());
            String encodedMsg = URLEncoder.encode(msg, StandardCharsets.UTF_8);
            return "redirect:/?jobName=%s&msg=%s".formatted(jobName, encodedMsg);
        }

    }

    @GetMapping("/jobs/restart/{jobName}")
    public String restartJob(@PathVariable String jobName) {

        jobService.restartJobAsync(jobName);
        String msg = "successfully restarted";
        String encodedMsg = URLEncoder.encode(msg, StandardCharsets.UTF_8);
        return "redirect:/?jobName=%s&msg=%s".formatted(jobName, encodedMsg);
    }

}