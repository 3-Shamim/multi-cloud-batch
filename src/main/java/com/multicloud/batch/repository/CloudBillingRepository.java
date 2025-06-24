package com.multicloud.batch.repository;

import com.multicloud.batch.model.CloudBilling;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

@Repository
public interface CloudBillingRepository extends JpaRepository<CloudBilling, Long> {
}
