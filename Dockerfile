#
# Build stage
#
FROM maven:3.6.0-jdk-11-slim AS build
COPY krakenTestCaseFinal.java /home/src/test/java/websockets/
COPY pom.xml /home/src
RUN mvn -f /home/src/pom.xml clean package

#
# Package stage
#
