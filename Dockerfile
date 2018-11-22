FROM gradle:4.10.2-jdk8 as builder
COPY --chown=gradle:gradle . /home/gradle/src
WORKDIR /home/gradle/src
RUN gradle build -Dorg.gradle.daemon=true

FROM openjdk:8u181-jre-alpine3.8
LABEL maintainer="Maxim Ivanov <me@quckly.ru>"
EXPOSE 10001

COPY --from=builder /home/gradle/src/build/libs/sne-dfs-master-0.0.1-SNAPSHOT.jar /app/
WORKDIR /app
CMD ["java", "-jar", "sne-dfs-master-0.0.1-SNAPSHOT.jar"]
