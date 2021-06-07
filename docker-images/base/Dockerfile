FROM registry.access.redhat.com/ubi8/ubi-minimal:latest
ARG JAVA_VERSION=11
ARG TARGETPLATFORM

USER root

RUN microdnf update \
    && microdnf install java-${JAVA_VERSION}-openjdk-headless java-${JAVA_VERSION}-openjdk-devel openssl procps shadow-utils \
    && microdnf clean all

ENV JAVA_HOME /usr/lib/jvm/jre-11

#####
# Add Tini
#####
ENV TINI_VERSION v0.19.0
ENV TINI_SHA256_AMD64=93dcc18adc78c65a028a84799ecf8ad40c936fdfc5f2a57b1acda5a8117fa82c
ENV TINI_SHA256_ARM64=07952557df20bfd2a95f9bef198b445e006171969499a1d361bd9e6f8e5e0e81
ENV TINI_SHA256_PPC64LE=3f658420974768e40810001a038c29d003728c5fe86da211cff5059e48cfdfde

RUN echo $TARGETPLATFORM

RUN set -ex; \
    if [[ ${TARGETPLATFORM} = "linux/ppc64le" ]]; then \
        curl -s -L https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-ppc64le -o /usr/bin/tini; \
        echo "${TINI_SHA256_PPC64LE} */usr/bin/tini" | sha256sum -c; \
        chmod +x /usr/bin/tini; \
    elif [[ ${TARGETPLATFORM} = "linux/arm64" ]]; then \
        curl -s -L https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-arm64 -o /usr/bin/tini; \
        echo "${TINI_SHA256_ARM64} */usr/bin/tini" | sha256sum -c; \
        chmod +x /usr/bin/tini; \
    else \
        curl -s -L https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini -o /usr/bin/tini; \
        echo "${TINI_SHA256_AMD64} */usr/bin/tini" | sha256sum -c; \
        chmod +x /usr/bin/tini; \
    fi
