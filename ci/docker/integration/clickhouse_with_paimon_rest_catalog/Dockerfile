# docker build -t clickhouse/integration-test-with-paimon-rest-catalog .
FROM maven:3.9-eclipse-temurin-11 AS build

WORKDIR /app
COPY paimon-rest-catalog/ .
RUN mvn clean package -DskipTests -B \
    && mv target/paimon-server-starter-1.0-SNAPSHOT.jar /app/paimon-server.jar

FROM eclipse-temurin:11-jre

COPY --from=build /app/paimon-server.jar /opt/paimon/paimon-server.jar
WORKDIR /opt/paimon

ENTRYPOINT ["java", "-jar", "/opt/paimon/paimon-server.jar"]
