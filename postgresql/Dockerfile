FROM maven:3.9.10-eclipse-temurin-21
ENV YCSB_VERSION=0.17.0 \
    PATH=${PATH}:/usr/bin

ENV YCSB_VERSION=0.17.0 \
    PATH=${PATH}:/usr/bin

WORKDIR /app

RUN apt-get update; \
    apt-get install -y --no-install-recommends python-is-python3 \
    && apt-get clean

COPY ./ycsb/ ./

RUN mvn -pl site.ycsb:jdbc-binding -am clean package

ADD https://jdbc.postgresql.org/download/postgresql-42.7.7.jar ./postgresql-connector-java.jar

RUN touch load-postgresql.dat
RUN touch transaction-postgresql.dat

# There are two reasons why this is split into another shell file:
# 1. the load phase needs to be ran at runtime, not build because otherwise it can't access the volume (therefore it's easier to have it a separate script rather than running all of it in the CMD statement)
# 2. the transaction phase fails when run in the CMD statement - for some reason the command gets interpreted wrong
COPY ./postgresql/workload.sh ./
RUN chmod +x ./workload.sh

COPY ./postgresql/db.properties ./


CMD ["./workload.sh"]
