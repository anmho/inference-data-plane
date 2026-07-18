FROM --platform=linux/amd64 us-central1-docker.pkg.dev/cloud-run-oss-images/crema-v1/autoscaler:1.0 AS crema

FROM --platform=linux/amd64 eclipse-temurin:21-jre
WORKDIR /app
COPY --from=crema /app/entrypoint.sh /app/entrypoint.sh
COPY --from=crema /app/logging.properties /app/logging.properties
COPY --from=crema /app/metric-provider /app/metric-provider
COPY --from=crema /app/scaler_server.jar /app/scaler_server.jar
RUN chmod +x /app/entrypoint.sh /app/metric-provider
CMD ["/app/entrypoint.sh"]
