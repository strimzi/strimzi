# OLM bundle generator
Bash script for generating an OLM bundle for OperatorHub from existing CRD, ClusterRole, Configmap, and Deployment files.
The generator copies and renames these existing files from the `../../packaging/install/cluster-operator` directory.

Then the generator creates a CSV file generating the following CSV fields automatically:
- `metadata.name`
- `metadata.annotations.containerImage`
- `metadata.annotations.createdAt`
- `spec.name`
- `spec.version`
- `spec.replaces`
- `spec.install.spec.permissions`
- `spec.install.spec.clusterPermissions`
- `spec.install.spec.deployments`
- `spec.relatedImages`

All other CSV fields must be updated manually in the template CSV file, `./csv-template/bases/strimzi-cluster-operator.clusterserviceversion.yaml`

## Requirements
- bash 5+ (GNU)
- yq 4.6+ (YAML processor)
- operator-sdk 1.6.2+
- skopeo 1.3.0

## Usage
```
make BUNDLE_VERSION=<BUNDLE_VERSION>

# Alternatively, to override image env vars taken from ../../Makefile.docker
make BUNDLE_VERSION=<BUNDLE_VERSION> \
     DOCKER_REGISTRY=<DOCKER_REGISTRY> \ 
     DOCKER_ORG=<DOCKER_ORG> \
     DOCKER_TAG=<DOCKER_TAG>
```

for example:
```
make BUNDLE_VERSION=0.23.0

# Alternatively, to override the image env vars taken from ../../Makefile.docker
make BUNDLE_VERSION=0.23.0 \
     DOCKER_REGISTRY=docker.io \
     DOCKER_ORG=strimzi \
     DOCKER_TAG=0.23.0
```
