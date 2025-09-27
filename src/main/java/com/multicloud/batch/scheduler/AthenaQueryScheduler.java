package com.multicloud.batch.scheduler;

import com.multicloud.batch.dao.aws.AthenaService;
import com.multicloud.batch.dao.aws.AwsSecretsManagerService;
import com.multicloud.batch.dao.aws.payload.SecretPayload;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.athena.AthenaClient;

import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

@Slf4j
@Component
@RequiredArgsConstructor
public class AthenaQueryScheduler {

    @Value("${aws.access_key}")
    private String accessKey;
    @Value("${aws.secret_key}")
    private String secretKey;
    @Value("${aws.region}")
    private String region;

    private final AwsSecretsManagerService awsSecretsManagerService;
    private final AthenaService athenaService;

//    @Scheduled(fixedDelay = 1, timeUnit = TimeUnit.DAYS)
    void storeSecrets() {

//        SecretPayload aws = new SecretPayload();
//        aws.setAccessKey("");
//        aws.setSecretKey("");
//        aws.setRegion(region);
//        awsSecretsManagerService.createSecret("azerion_mc/billing/secrets/aws", aws, true);

//        SecretPayload internalHuawei = new SecretPayload();
//        internalHuawei.setDomainName("");
//        internalHuawei.setUsername("");
//        internalHuawei.setPassword("");
//        internalHuawei.setRegion("");
//        awsSecretsManagerService.createSecret("azerion_mc/billing/secrets/internal/huawei", internalHuawei, true);

//        SecretPayload externalHuawei = new SecretPayload();
//        externalHuawei.setAccessKey("");
//        externalHuawei.setSecretKey("");
//        externalHuawei.setRegion("");
//        awsSecretsManagerService.createSecret("azerion_mc/billing/secrets/external/huawei", externalHuawei, true);

//        SecretPayload internalGcp = new SecretPayload();
//        internalGcp.setJsonKey("");
//        awsSecretsManagerService.createSecret("azerion_mc/billing/secrets/internal/gcp", internalGcp, true);

    }

//    @Scheduled(fixedDelay = 1, timeUnit = TimeUnit.DAYS)
    void runAthenaQuery() {

        StaticCredentialsProvider provider = StaticCredentialsProvider.create(
                AwsBasicCredentials.create(accessKey, secretKey)
        );

        AthenaClient client = AthenaClient.builder()
                .credentialsProvider(provider)
                .region(Region.of(region))
                .build();

        String bucket = "azerion-athena-results";
        String prefix = "azerion_mc";

        String outputLocation = "s3://%s/%s/".formatted(bucket, prefix);
        String externalDB = "abc_cur_exports";
        String internalDB = "athenacurcfn_athena";

        String query = """
                SELECT
                  line_item_product_code,
                  ROUND(SUM(line_item_unblended_cost), 2) as total_cost,
                  ROUND(SUM(pricing_public_on_demand_cost), 2) as on_demand_cost,
                  month, year
                FROM cur_azul
                WHERE year = '2025' and month = '9'
                GROUP BY 1, 4, 5
                ORDER BY total_cost DESC
                """;

        query = """
                SELECT DATE(line_item_usage_start_date)                                      AS usage_date,
                    COALESCE(line_item_line_item_type, 'UNKNOWN')                            AS billing_type,
                    COUNT(*)                                                                 AS rows,
                    CAST(COALESCE(SUM(line_item_unblended_cost), 0) AS DECIMAL(20, 8))       AS unblended_cost
                FROM athena
                WHERE year = '2025' and month = '8'
                    AND product_servicecode = ''
                    AND DATE(line_item_usage_start_date) >= DATE '2025-08-01'
                    AND date(line_item_usage_start_date) <= DATE '2025-08-07'
                GROUP BY 1, 2
                ORDER BY 1;
                """;

//        query = "show tables";
//        query = "select count(*) from %s where year = '2025'";
//        query = "select * from cur_azul where month = '9' and year = '2025' limit 1";
//        query = """
//            select distinct(billing_entity)
//            from athena a
//            left join org_accounts o ON a.line_item_usage_account_id = o.account_id
//            where month = '8' and year = '2025'
//        """;

        Set<String> externalTables = Set.of(
                "cur_azul", "cur_bbw", "cur_da", "cur_lidion", "cur_nimbus", "cur_refine", "cur_stratego_billing_group",
                "cur_team_acity", "cur_team_apex", "cur_team_artemis", "cur_team_feig"
        );

        Set<String> internalTables = Set.of("athena");

        for (String table : internalTables) {

//            query = String.format("select count(*) from %s where year = '2025'", table);

            String executionId = athenaService.submitAthenaQuery(query, outputLocation, internalDB, client);
            athenaService.waitForQueryToComplete(executionId, client);

            System.out.println(table);
            athenaService.printQueryResults(executionId, client);

            System.out.println();

        }

    }

}


