package com.multicloud.batch.job;

import com.multicloud.batch.dao.aws.AwsSecretsManagerService;
import com.multicloud.batch.dao.aws.payload.SecretPayload;
import com.multicloud.batch.dto.PerDayCostDTO;
import com.multicloud.batch.dto.ProductDTO;
import com.multicloud.batch.enums.CloudProvider;
import com.multicloud.batch.secondary.model.CustomerDailyCost;
import com.multicloud.batch.service.AzerionCostStoreService;
import com.multicloud.batch.service.CustomerCostService;
import com.multicloud.batch.service.SecretPayloadStoreService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.Date;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

@Slf4j
@Configuration
@ConditionalOnExpression("${batch_job.customer_cost.enabled}")
public class CalculateCustomerCostJobConfig {

    private static final String SECRET_STORE_KEY = "global_aws_billing_data_secret";

    @Value("${batch_job.aws_billing_data.secret_path}")
    private String awsSecretPath;

    private final DataSource dataSource;

    private final JobRepository jobRepository;

    private final JdbcTemplate secondaryJdbcTemplate;
    private final PlatformTransactionManager secondaryTransactionManager;

    private final CustomerCostService customerCostService;
    private final SecretPayloadStoreService secretPayloadStoreService;
    private final AwsSecretsManagerService awsSecretsManagerService;
    private final AzerionCostStoreService azerionCostStoreService;

    public CalculateCustomerCostJobConfig(DataSource dataSource,
                                          @Qualifier(value = "secondaryJdbcTemplate")
                                          JdbcTemplate secondaryJdbcTemplate,
                                          @Qualifier(value = "secondaryTransactionManager")
                                          PlatformTransactionManager secondaryTransactionManager,
                                          JobRepository jobRepository,
                                          CustomerCostService customerCostService,
                                          SecretPayloadStoreService secretPayloadStoreService,
                                          AwsSecretsManagerService awsSecretsManagerService,
                                          AzerionCostStoreService azerionCostStoreService) {

        this.dataSource = dataSource;
        this.secondaryJdbcTemplate = secondaryJdbcTemplate;
        this.jobRepository = jobRepository;
        this.secondaryTransactionManager = secondaryTransactionManager;
        this.customerCostService = customerCostService;
        this.secretPayloadStoreService = secretPayloadStoreService;
        this.awsSecretsManagerService = awsSecretsManagerService;
        this.azerionCostStoreService = azerionCostStoreService;
    }

    @Bean
    public Job calculateCustomerCostJob() {

        return new JobBuilder("calculateCustomerCostJob", jobRepository)
                .start(retrieveAzerionCostForAllExceptionalClientsStep())
                .next(calculateCustomerCostStep())
                .build();
    }

    @Bean
    public Step retrieveAzerionCostForAllExceptionalClientsStep() {

        return new StepBuilder("retrieveAzerionCostForAllExceptionalClientsStep", jobRepository)
                .tasklet((contribution, chunkContext) -> {

                    SecretPayload secret = secretPayloadStoreService.get(SECRET_STORE_KEY);

                    if (secret == null) {

                        secret = awsSecretsManagerService.getSecret(awsSecretPath, true);

                        if (secret == null) {
                            throw new RuntimeException("Secret not found for awsBillingDataJob");
                        }

                        // Store secret
                        secretPayloadStoreService.put(SECRET_STORE_KEY, secret);

                    }

                    azerionCostStoreService.fetchAndStoreAzerionCost(secret);

                    return RepeatStatus.FINISHED;
                }, secondaryTransactionManager).build();
    }

    @Bean
    public Step calculateCustomerCostStep() {

        return new StepBuilder("calculateCustomerCostStep", jobRepository)
                .<ProductDTO, ProductDTO>chunk(1, secondaryTransactionManager)
                .reader(customerCostProductReader())
                .writer(chunk -> {

                    LocalDate end = LocalDate.now();
                    LocalDate start = end.minusMonths(4).withDayOfMonth(1);

                    for (ProductDTO productDTO : chunk.getItems()) {

                        log.info(
                                "Calculating customer cost for product: {}, organization: {}, external: {} exceptional: {}",
                                productDTO.productName(),
                                productDTO.organizationName(),
                                productDTO.isInternalOrg(),
                                productDTO.isExceptionalOrg()
                        );

                        List<PerDayCostDTO> customerCostList = customerCostService.findPerDayCustomerCost(
                                productDTO.productId(),
                                productDTO.organizationId(),
                                productDTO.isInternalOrg(),
                                start,
                                end
                        );

                        List<CustomerDailyCost> customerDailyCostList = new ArrayList<>();

                        if (productDTO.isExceptionalOrg()) {
                            handleAllExceptionalCase(
                                    productDTO, customerCostList, customerDailyCostList
                            );
                        } else {
                            handleAllRegularCases(productDTO, start, end, customerCostList, customerDailyCostList);
                        }

                        insertCosts(customerDailyCostList);

                    }

                })
                .build();

    }

    @Bean
    public ItemReader<ProductDTO> customerCostProductReader() {

        JdbcCursorItemReader<ProductDTO> reader = new JdbcCursorItemReader<>();
        reader.setDataSource(dataSource);
        reader.setSql("""
                    SELECT p.id, p.name, o.id AS org_id, o.name AS org_name, o.internal, o.exceptional
                    FROM products p
                        JOIN organizations o ON p.organization_id = o.id
                    WHERE o.internal = false
                """);
        reader.setFetchSize(500);
        reader.setSaveState(false);
        reader.setVerifyCursorPosition(false);

        reader.setRowMapper((rs, rowNum) -> new ProductDTO(
                rs.getLong("id"),
                rs.getString("name"),
                rs.getLong("org_id"),
                rs.getString("org_name"),
                rs.getBoolean("internal"),
                rs.getBoolean("exceptional")
        ));

        try {
            reader.afterPropertiesSet();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return reader;
    }

    private void insertCosts(List<CustomerDailyCost> costs) {

        String query = """
                INSERT INTO customer_daily_cost(
                    day, mc_org_id, mc_org_name, customer_name, cloud_provider, azerion_cost, customer_cost, external
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON DUPLICATE KEY UPDATE
                    azerion_cost = VALUES(azerion_cost),
                    customer_cost = VALUES(customer_cost);
                """;

        secondaryJdbcTemplate.batchUpdate(query, costs, 100, (ps, cost) -> {
            ps.setDate(1, Date.valueOf(cost.getDay()));
            ps.setLong(2, cost.getMcOrgId());
            ps.setString(3, cost.getMcOrgName());
            ps.setString(4, cost.getCustomerName());
            ps.setString(5, cost.getCloudProvider().name());
            ps.setBigDecimal(6, cost.getAzerionCost());
            ps.setBigDecimal(7, cost.getCustomerCost());
            ps.setBoolean(8, cost.isExternal());
        });

    }

    private void handleAllRegularCases(ProductDTO productDTO, LocalDate start, LocalDate end,
                                       List<PerDayCostDTO> customerCostList,
                                       List<CustomerDailyCost> customerDailyCostList) {

        Map<LocalDate, BigDecimal> perDayAwsMap = new HashMap<>();
        Map<LocalDate, BigDecimal> outsideAwsMap = new HashMap<>();

        List<PerDayCostDTO> perDayAzerionCost = customerCostService.findPerDayAzerionCost(
                productDTO.productId(),
                productDTO.organizationId(),
                start,
                end
        );

        perDayAzerionCost.forEach(perDay -> perDayAwsMap.put(
                perDay.usageDate(), perDay.cost()
        ));

        List<PerDayCostDTO> azerionOutsideCost = customerCostService.findOutsideOfMonthAzerionCost(
                productDTO.productId(),
                productDTO.organizationId(),
                start
        );

        azerionOutsideCost.forEach(perDay -> outsideAwsMap.put(
                perDay.usageDate(), perDay.cost()
        ));

        for (PerDayCostDTO dto : customerCostList) {

            BigDecimal azerionCost = BigDecimal.ZERO;

            // Azerion get a 55% discount from Huawei
            if (dto.cloudProvider().equals(CloudProvider.HWC)) {
                azerionCost = dto.cost().multiply(BigDecimal.valueOf(0.45));
            }

            if (dto.cloudProvider().equals(CloudProvider.AWS)) {

                if (perDayAwsMap.containsKey(dto.usageDate())) {
                    azerionCost = azerionCost.add(perDayAwsMap.get(dto.usageDate()));
                }

                if (outsideAwsMap.containsKey(dto.usageDate())) {
                    azerionCost = azerionCost.add(outsideAwsMap.get(dto.usageDate()));
                }

            }

            if (dto.cloudProvider().equals(CloudProvider.GCP)) {
                azerionCost = dto.afterDiscountCost();
            }

            customerDailyCostList.add(
                    CustomerDailyCost.builder()
                            .day(dto.usageDate())
                            .mcOrgId(productDTO.organizationId())
                            .mcOrgName(productDTO.organizationName())
                            .customerName(productDTO.productName())
                            .cloudProvider(dto.cloudProvider())
                            .azerionCost(azerionCost)
                            .customerCost(
                                    dto.afterDiscountCost().add(dto.handlingFee()).add(dto.supportFee())
                            )
                            .external(!productDTO.isInternalOrg())
                            .build()
            );

        }

    }

    // We check exceptional for AWS only
    private void handleAllExceptionalCase(ProductDTO productDTO, List<PerDayCostDTO> customerCostList,
                                          List<CustomerDailyCost> customerDailyCostList) {

        // <ProductID, BillingEntity>
        Map<Long, String> awsBillingEntityMap = new HashMap<>();
        awsBillingEntityMap.put(18L, "hitta");
        awsBillingEntityMap.put(19L, "woozworld");
        awsBillingEntityMap.put(20L, "gembly-bv");
        awsBillingEntityMap.put(21L, "hkts");
        awsBillingEntityMap.put(33L, "whow-games-gmbh");
        awsBillingEntityMap.put(35L, "adinmo");

        for (PerDayCostDTO dto : customerCostList) {

            // For GCP
            BigDecimal azerionCost = dto.afterDiscountCost();

            // Azerion get a 55% discount from Huawei
            if (dto.cloudProvider().equals(CloudProvider.HWC)) {
                azerionCost = dto.cost().multiply(BigDecimal.valueOf(0.45));
            }

            // Aws
            if (dto.cloudProvider().equals(CloudProvider.AWS)) {

                String billingEntity = awsBillingEntityMap.get(productDTO.productId());

                if (billingEntity == null) {
                    azerionCost = BigDecimal.ZERO;
                } else {
                    azerionCost = azerionCostStoreService.getAzerionCost(dto.usageDate(), billingEntity);
                }

            }

            customerDailyCostList.add(
                    CustomerDailyCost.builder()
                            .day(dto.usageDate())
                            .mcOrgId(productDTO.organizationId())
                            .mcOrgName(productDTO.organizationName())
                            .customerName(productDTO.productName())
                            .cloudProvider(dto.cloudProvider())
                            .azerionCost(azerionCost)
                            .customerCost(
                                    dto.afterDiscountCost().add(dto.handlingFee()).add(dto.supportFee())
                            )
                            .external(!productDTO.isInternalOrg())
                            .build()
            );

        }

    }

}
