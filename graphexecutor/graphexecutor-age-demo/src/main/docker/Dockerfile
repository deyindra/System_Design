# A demo worker over a graph in Apache AGE. Build context: the module directory, after `mvn package`
# (which puts the demo jar in target/ and its runtime dependencies, graphexecutor-age among them, in target/lib/).
FROM eclipse-temurin:17-jre
WORKDIR /app
COPY target/lib/ lib/
COPY target/graphexecutor-age-demo.jar ./
ENTRYPOINT ["java", "-cp", "/app/graphexecutor-age-demo.jar:/app/lib/*"]
# configured by environment: see DemoWorkerMain; WORKER_FACTORY is one of the demo package's factories
CMD ["com.salesforce.einstein.graphexecutor.age.demo.DemoWorkerMain"]
