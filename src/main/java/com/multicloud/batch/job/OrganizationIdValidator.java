package com.multicloud.batch.job;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.JobParametersValidator;
import org.springframework.stereotype.Component;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

@Slf4j
@Component
public class OrganizationIdValidator implements JobParametersValidator {

    @Override
    public void validate(JobParameters parameters) throws JobParametersInvalidException {

        if (parameters == null) {
            throw new JobParametersInvalidException("Job parameters are required for mergeBillingDataJob");
        }

        Long orgId = parameters.getLong("orgId");

        if (orgId == null) {
            throw new JobParametersInvalidException("Organization ID is required for mergeBillingDataJob");
        }

    }

}
