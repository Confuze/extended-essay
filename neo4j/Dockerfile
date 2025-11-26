FROM maven:3.9.10-eclipse-temurin-21
ENV YCSB_VERSION=0.17.0 \
    PATH=${PATH}:/usr/bin

WORKDIR /app

RUN apt-get update; \
    apt-get install -y --no-install-recommends python-is-python3 \
    && apt-get clean

COPY ./ycsb/ ./

RUN mvn -pl site.ycsb:neo4j-binding -am clean package

RUN touch load-neo4j.dat
RUN touch transaction-neo4j.dat

# There are two reasons why this is split into another shell file:
# 1. the load phase needs to be ran at runtime, not build because otherwise it can't access the volume (therefore it's easier to have it a separate script rather than running all of it in the CMD statement)
# 2. the transaction phase fails when run in the CMD statement - for some reason the command gets interpreted wrong

COPY ./neo4j/ycsb_workload.sh ./
RUN chmod +x ycsb_workload.sh

COPY ./neo4j/db.properties ./

CMD ["./ycsb_workload.sh"]
