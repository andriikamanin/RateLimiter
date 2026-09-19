####################################
# Build stage
####################################
FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /build

# Cache dependencies separately from source for faster incremental builds.
COPY pom.xml .
RUN mvn -q -B dependency:go-offline

COPY src ./src
RUN mvn -q -B package -DskipTests

####################################
# Runtime stage
####################################
FROM eclipse-temurin:21-jre-jammy
WORKDIR /app

RUN useradd --system --create-home --shell /usr/sbin/nologin ratelimiter
COPY --from=build /build/target/rate-limiter-service-*.jar /app/rate-limiter-service.jar
USER ratelimiter

EXPOSE 6565 8081

# Generational ZGC keeps GC pauses sub-millisecond even under the
# allocation churn of a high-throughput, high-concurrency gRPC service
# running on virtual threads.
ENV JAVA_OPTS="-XX:+UseZGC -XX:+ZGenerational -Xms256m -Xmx512m"

ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/rate-limiter-service.jar"]
