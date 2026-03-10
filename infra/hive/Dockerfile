FROM apache/hive:4.0.0

USER root

ARG HADOOP_AWS_VERSION=3.3.6
ARG AWS_JAVA_SDK_BUNDLE_VERSION=1.12.367

RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

RUN curl -L -o /opt/hive/lib/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar

RUN curl -L -o /opt/hive/lib/aws-java-sdk-bundle-${AWS_JAVA_SDK_BUNDLE_VERSION}.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_JAVA_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_JAVA_SDK_BUNDLE_VERSION}.jar