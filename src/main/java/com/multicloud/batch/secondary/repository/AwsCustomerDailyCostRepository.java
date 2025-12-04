package com.multicloud.batch.secondary.repository;

import com.multicloud.batch.secondary.model.AwsCustomerDailyCost;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

@Repository
public interface AwsCustomerDailyCostRepository extends JpaRepository<AwsCustomerDailyCost, Long> {


}
