FROM eclipse-temurin:21-jdk AS build
WORKDIR /workspace
COPY . .
RUN ./mvnw -DskipTests package

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /workspace/target/kay-ledger-api-0.0.1-SNAPSHOT.jar /app/kay-ledger-api.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/kay-ledger-api.jar"]
