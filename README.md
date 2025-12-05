# Multi-Cloud Batch

## Overview
This is the batch service for the Multi-Cloud platform.  
It uses **MariaDB** as the primary database and supports running locally or via Docker.

---

## 📦 Requirements
- **Java 21+**
- **Maven 3.9+** (or use the included `mvnw` wrapper)
- **MariaDB** (local or containerized)
- **Docker & Docker Compose** (if running via containers)
---

## 🗄 Database
- **Type:** MariaDB
- **Default Name:** `multi_cloud`
- Automatically created if it does not exist (when `createDatabaseIfNotExist=true` is set in the JDBC URL).

## 🚀 Running Locally

### 1. Create Local Config
Inside `src/main/resources`, create a file with name `application-dev.properties`

* If you don't set anything for AWS config, then it will try `.aws/credentials`'s default profile.

Paste the following (update the values as needed):

```properties
aws.access_key=
aws.secret_key=
aws.session_token=
aws.region=eu-west-1
aws.profile=
aws.secret.prefix=local/azerion_mc/backend

spring.datasource.url=jdbc:mariadb://localhost:3306/multi_cloud?createDatabaseIfNotExist=true&useSSL=false&allowPublicKeyRetrieval=true&useLegacyDatetimeCode=false&useUnicode=yes&characterEncoding=UTF-8&serverTimezone=UTC
spring.datasource.username=root
spring.datasource.password=root
spring.jpa.show-sql=true

# Cron Expression for Batch Jobs,
# By default, data-related jobs are disabled for dev profile.
# To enable them, set enabled=true
# Don't do any change in prod and dev profiles.
batch_job.aws_billing_data.corn=0 28 * * * *
batch_job.aws_billing_data.enabled=false
batch_job.external_aws_billing_data.corn=0 56 * * * *
batch_job.external_aws_billing_data.enabled=false
batch_job.exceptional_aws_billing_data.corn=0 25 * * * *
batch_job.exceptional_aws_billing_data.enabled=false
batch_job.gcp_billing_data.corn=15 28 * * * *
batch_job.gcp_billing_data.enabled=false
batch_job.huawei_billing_data.corn=30 28 * * * *
batch_job.huawei_billing_data.enabled=false
batch_job.external_huawei_billing_data.corn=15 56 * * * *
batch_job.external_huawei_billing_data.enabled=false
batch_job.merge_billing.corn=0 39 * * * *
batch_job.merge_billing.enabled=false
batch_job.monthly_invoice.corn=0 8 * * * *
batch_job.monthly_invoice.enabled=false
batch_job.daily_org_pricing_update.corn=0 3 * * * *
batch_job.daily_org_pricing_update.enabled=false
batch_job.aws_customer_cost.corn=0 0 0 1 * *
batch_job.aws_customer_cost.enabled=true
```

💡 Tip: Instead of creating this file, you can define `_env`'s variables as environment variables in IntelliJ’s Run/Debug Configuration.

## ⚙️ Build & Run - *.jar

* Build
    * `./mvnw clean install`
    * `mvn clean install`
* Run
    * `java -jar /target/multi-cloud-batch.jar`

## 🐳 Docker

`Make sure you have copied _env to .env and update the variables`

* Build
    * `mvn clean install`
    * `docker compose build`
* Run
    * `docker compose up`
    * `docker compose up -d`
    * Make `network_mode: host` if you want to connect local DB