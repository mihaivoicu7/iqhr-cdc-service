FROM eclipse-temurin:21-jre
WORKDIR /app
RUN groupadd --gid 10001 cdc && useradd --uid 10001 --gid cdc --no-create-home cdc
COPY --chown=10001:10001 target/iqhr-cdc-service-0.1.0.jar /app/service.jar
COPY --chown=10001:10001 config/application.example.yml /app/config/application.yml
USER 10001:10001
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=70 -XX:+ExitOnOutOfMemoryError -Duser.timezone=UTC"
ENTRYPOINT ["java","-jar","/app/service.jar"]
