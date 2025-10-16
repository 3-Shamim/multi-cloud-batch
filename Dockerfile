FROM openjdk:21-jdk-slim as builder
LABEL creator="Md Shamim"
LABEL email="shamim.molla@vivasoftltd.com"
WORKDIR /app
COPY /target/multi-cloud-batch.jar application.jar
RUN java -Djarmode=layertools -jar application.jar extract

FROM openjdk:21-jdk-slim
WORKDIR /app
COPY --from=builder /app/dependencies/ ./
COPY --from=builder /app/spring-boot-loader/ ./
COPY --from=builder /app/snapshot-dependencies/ ./
COPY --from=builder /app/application/ ./

EXPOSE 8080

ENTRYPOINT ["java", "org.springframework.boot.loader.launch.JarLauncher"]