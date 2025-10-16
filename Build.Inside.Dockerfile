# Build stage
FROM maven:3.9.6-eclipse-temurin-21 AS maven_builder
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn clean package -DskipTests=true

# Extract layers
FROM eclipse-temurin:21-jre AS layertools
WORKDIR /layers
COPY --from=maven_builder /app/target/multi-cloud-batch.jar app.jar
RUN java -Djarmode=layertools -jar app.jar extract

# Runtime image - JRE
FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=layertools /layers/dependencies/ ./
COPY --from=layertools /layers/snapshot-dependencies/ ./
COPY --from=layertools /layers/spring-boot-loader/ ./
COPY --from=layertools /layers/application/ ./

EXPOSE 8080

ENTRYPOINT ["java", "org.springframework.boot.loader.launch.JarLauncher"]