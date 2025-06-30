package com.multicloud.batch.dao.aws;

import com.multicloud.batch.cloud_config.aws.AwsDynamicCredentialsProvider;
import com.multicloud.batch.enums.CloudProvider;
import com.multicloud.batch.enums.LastSyncStatus;
import com.multicloud.batch.model.CloudDailyBilling;
import com.multicloud.batch.repository.CloudDailyBillingRepository;
import jakarta.persistence.EntityManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.util.Pair;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.services.costexplorer.CostExplorerClient;
import software.amazon.awssdk.services.costexplorer.model.*;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

@Slf4j
@RequiredArgsConstructor
@Service
public class AwsBillingServiceImpl implements AwsBillingService {

    private final EntityManager entityManager;
    private final CostExplorerClient costExplorerClient;
    private final CloudDailyBillingRepository cloudDailyBillingRepository;

    @Override
    public Pair<LastSyncStatus, String> fetchDailyServiceCostUsage(long organizationId, String accessKey,
                                                                   String secretKey, LastSyncStatus lastSyncStatus) {

        AwsBasicCredentials credentials = AwsBasicCredentials.create(accessKey, secretKey);
        AwsDynamicCredentialsProvider.setAwsCredentials(credentials);

        try {

            LocalDate start;

            if (lastSyncStatus == null) {
                start = YearMonth.now().minusMonths(12).atDay(1);
            } else {
                start = LocalDate.now().minusDays(7);
            }

            LocalDate end = LocalDate.now();

            GetCostAndUsageRequest request = GetCostAndUsageRequest.builder()
                    .timePeriod(
                            DateInterval.builder()
                                    .start(start.format(DateTimeFormatter.ISO_DATE))
                                    .end(end.format(DateTimeFormatter.ISO_DATE))
                                    .build()
                    )
                    .granularity(Granularity.DAILY)
                    .metrics(Metric.UNBLENDED_COST.name())
                    .groupBy(
                            GroupDefinition.builder().type(GroupDefinitionType.DIMENSION).key(Dimension.SERVICE.name()).build()

                    )
                    .build();

            GetCostAndUsageResponse response = costExplorerClient.getCostAndUsage(request);

            List<CloudDailyBilling> billings = new ArrayList<>();

            for (ResultByTime result : response.resultsByTime()) {

                LocalDate startDate = LocalDate.parse(result.timePeriod().start());

                for (Group group : result.groups()) {

                    String service = group.keys().getFirst();

                    BigDecimal cost = new BigDecimal(group.metrics().get("UnblendedCost").amount());

                    CloudDailyBilling billing = CloudDailyBilling.builder()
                            .organizationId(organizationId)
                            .cloudProvider(CloudProvider.AWS)
                            .serviceName(service)
                            .date(startDate)
                            .costAmountUsd(cost)
                            .currency("USD")
                            .billingExportSource("AWSCostExplorer")
                            .build();

                    billings.add(billing);

                }

            }

            cloudDailyBillingRepository.batchUpsert(billings, entityManager);

            return Pair.of(LastSyncStatus.SUCCESS, "Successfully synced [%d] items.".formatted(billings.size()));

        } catch (Exception e) {
            log.error("AWS billing data fetch error", e);
            return Pair.of(LastSyncStatus.FAIL, e.getMessage());
        } finally {
            AwsDynamicCredentialsProvider.clear();
        }

    }

//    @Override
//    public void fetchDailyServiceCostUsage(String accessKey, String secretKey, long organizationId) {
//
//        AwsBasicCredentials credentials = AwsBasicCredentials.create(accessKey, secretKey);
//        AwsDynamicCredentialsProvider.setAwsCredentials(credentials);
//
//        try {
//
//            LocalDateTime now = LocalDateTime.now();
//
//            LocalDate start = YearMonth.now().minusMonths(12).atDay(1);
//            LocalDate end = LocalDate.now();
//
//            GetCostAndUsageRequest request = GetCostAndUsageRequest.builder()
//                    .timePeriod(
//                            DateInterval.builder()
//                                    .start(start.format(DateTimeFormatter.ISO_DATE))
//                                    .end(end.format(DateTimeFormatter.ISO_DATE))
//                                    .build()
//                    )
//                    .granularity(Granularity.DAILY)
//                    .metrics(Metric.UNBLENDED_COST.name(), Metric.USAGE_QUANTITY.name())
//                    .groupBy(
//                            GroupDefinition.builder().type(GroupDefinitionType.DIMENSION).key(Dimension.SERVICE.name()).build(),
//                            GroupDefinition.builder().type(GroupDefinitionType.DIMENSION).key(Dimension.USAGE_TYPE.name()).build()
//
//                    )
//                    .build();
//
//            GetCostAndUsageResponse response = costExplorerClient.getCostAndUsage(request);
//
//            List<CloudBilling> billings = new ArrayList<>();
//
//            for (ResultByTime result : response.resultsByTime()) {
//
//                LocalDate startDate = LocalDate.parse(result.timePeriod().start());
//                LocalDate endDate = LocalDate.parse(result.timePeriod().end());
//
//                for (Group group : result.groups()) {
//
//                    String service = group.keys().getFirst();
//                    String usageType = group.keys().get(1);
//
//                    BigDecimal usage = new BigDecimal(group.metrics().get("UsageQuantity").amount());
//                    BigDecimal cost = new BigDecimal(group.metrics().get("UnblendedCost").amount());
//
//                    CloudBilling billing = CloudBilling.builder()
//                            .organizationId(1)
//                            .cloudProvider(CloudProvider.AWS)
//                            .serviceName(service)
//                            .skuName(usageType)
//                            .usageStartDate(startDate)
//                            .usageEndDate(endDate)
//                            .usageAmount(usage)
//                            .usageUnit(null)
//                            .costAmountUsd(cost)
//                            .currency("USD")
//                            .billingExportSource("AWSCostExplorer")
//                            .build();
//
//                    billings.add(billing);
//
//                }
//
//            }
//
//            System.out.println("=============================================");
//
//            System.out.println("Fetch time: " + Duration.between(now, LocalDateTime.now()).toSeconds());
//
//            now = LocalDateTime.now();
//
//            System.out.println("Total count: " + billings.size());
//            cloudBillingRepository.saveAll(billings);
//
//            System.out.println("Save time: " + Duration.between(now, LocalDateTime.now()).toSeconds());
//
//            System.out.println("=============================================");
//
//        } finally {
//            AwsDynamicCredentialsProvider.clear();
//        }
//
//    }

    @Override
    public boolean checkAwsExplorerConnection(String accessKey, String secretKey) {

        AwsBasicCredentials credentials = AwsBasicCredentials.create(accessKey, secretKey);
        AwsDynamicCredentialsProvider.setAwsCredentials(credentials);

        try {

            LocalDate end = LocalDate.now();
            LocalDate start = end.minusDays(1);

            GetCostAndUsageRequest request = GetCostAndUsageRequest.builder()
                    .timePeriod(DateInterval.builder()
                            .start(start.format(DateTimeFormatter.ISO_DATE))
                            .end(end.format(DateTimeFormatter.ISO_DATE))
                            .build())
                    .granularity(Granularity.DAILY)
                    .metrics("BlendedCost")
                    .build();

            costExplorerClient.getCostAndUsage(request); // If this doesn't throw, access is working
            costExplorerClient.close();

            return true;
        } catch (Exception e) {
            log.error("Cost Explorer error: {}", e.getMessage());
            return false;
        } finally {
            AwsDynamicCredentialsProvider.clear();
        }

    }

}
