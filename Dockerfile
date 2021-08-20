FROM itpnt-registry.op.redhat.com/ubi8-jdk11-runtime
WORKDIR /app
COPY --chown=1000:1000 target/*.jar ./silverbroccoli.jar
USER 1000
CMD ["java", "-jar", "/app/silverbroccoli.jar"]
