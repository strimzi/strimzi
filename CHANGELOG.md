# CHANGELOG

## 0.25.0

* Move from Scala 2.12 to Scala 2.13. (#5192)
* Open Policy Agent authorizer updated to a new version supporting Scala 2.13. See the _Changes, deprecations and removals_ sections for more details. (#5192)
* Allow a custom password to be set for SCRAM-SHA-512 users by referencing a secret in the `KafkaUser` resource
* Add support for `tls-external` authentication to User Operator to allow management of ACLs and Quotas for TLS users with user certificates generated externally (#5249) 
* Support for disabling the automatic generation of network policies by the Cluster Operator. Set the Cluster Operator's `STRIMZI_NETWORK_POLICY_GENERATION` environment variable to `false` to disable network policies. (#5258)
* Annotate Cluster Operator resource metrics by a namespace label

### Changes, deprecations and removals

* The `KafkaConnectS2I` resource has been removed and is no longer supported by the operator.
  Please use the [migration guide](https://strimzi.io/docs/operators/0.24.0/full/using.html#proc-migrating-kafka-connect-s2i-str) to migrate your `KafkaConnectS2I` deployments to [`KafkaConnect` Build](https://strimzi.io/docs/operators/latest/full/deploying.html#creating-new-image-using-kafka-connect-build-str) instead.
* The Open Policy Agent authorizer has been updated to a new version that supports Scala 2.13.
  The new release introduces a new format of the input data sent to the Open Policy Agent server.
  For more information about the new format and how to migrate from the old version, see the [OPA Kafka plugin v1.0.0 release notes](https://github.com/Bisnode/opa-kafka-plugin/releases/tag/v1.0.0).

## 0.24.0

* Add support for [Kubernetes Configuration Provider for Apache Kafka](https://github.com/strimzi/kafka-kubernetes-config-provider)
* Use Red Hat UBI8 base image
* Add support for Kafka 2.7.1 and remove support for 2.6.0, 2.6.1, and 2.6.2
* Support for patching of service accounts and configuring their labels and annotations. The feature is disabled by default and enabled using the new `ServiceAccountPatching` feature gate.
* Added support for configuring cluster-operator's worker thread pool size that is used for various sync and async tasks
* Add Kafka Quotas plugin with produce, consume, and storage quotas
* Support pausing reconciliation of KafkaTopic CR with annotation `strimzi.io/pause-reconciliation`
* Update cruise control to 2.5.57
* Update to Strimzi Kafka Bridge to 0.20.0
* Support for broker load information added to the rebalance optimization proposal. Information on the load difference, before and after a rebalance is stored in a ConfigMap
* Add support for selectively changing the verbosity of logging for individual CRs, using markers.
* Added support for `controller_mutation_rate' quota. Creation/Deletion of topics and creation of partitions can be configured through this.
* Use newer version of Kafka Exporter with different bugfixes 

### Changes, deprecations and removals

* The deprecated `KafkaConnectS2I` custom resource will be removed after the 0.24.0 release. 
  Please use the [migration guide](https://strimzi.io/docs/operators/latest/full/using.html#proc-migrating-kafka-connect-s2i-str) to migrate your `KafkaConnectS2I` deployments to [`KafkaConnect` Build](https://strimzi.io/docs/operators/latest/full/deploying.html#creating-new-image-using-kafka-connect-build-str) instead.
* The fields `topicsBlacklistPattern` and `groupsBlacklistPattern` in the `KafkaMirrorMaker2` resource are deprecated and will be removed in the future.
  They are replaced by new fields `topicsExcludePattern` and `groupsExcludePattern`.
* The field `whitelist` in the `KafkaMirrorMaker` resource is deprecated and will be removed in the future.
  It is replaced with a new field `include`.
* `bind-utils` removed from containers to improve security posture.
* Kafka Connect Build now uses hashes to name downloaded artifact files. Previously, it was using the last segment of the download URL.
  If your artifact requires a specific name, you can use the new `type: other` artifact and its `fileName` field.
* The option `enableECDSA` of Kafka CR `authentication` of type `oauth` has been deprecated and is ignored. 
  ECDSA token signature support is now always enabled without the need for Strimzi Cluster Operator installing the BouncyCastle JCE crypto provider. 
  BouncyCastle library is no longer packaged with Strimzi Kafka images.

## 0.23.0

* Add support for Kafka 2.8.0 and 2.6.2, remove support for Kafka 2.5.x
* Make it possible to configure maximum number of connections and maximum connection creation rate in listener configuration
* Add support for configuring finalizers for `loadbalancer` type listeners
* Use dedicated Service Account for Kafka Connect Build on Kubernetes 
* Remove direct ZooKeeper access for handling user quotas in the User Operator. Add usage of Admin Client API instead.
* Migrate to CRD v1 (required by Kubernetes 1.22+)
* Support for configuring custom Authorizer implementation 
* Changed Reconciliation interval for Topic Operator from 90 to 120 seconds (to keep it the same as for other operators)
* Changed Zookeeper session timeout default value to 18 seconds for Topic and User Operators (for improved resiliency)
* Removed requirement for replicas and partitions KafkaTopic spec making these parameters optional
* Support to configure a custom filter for parent CR's labels propagation into subresources 
* Allow disabling service links (environment variables describing Kubernetes services) in Pod template
* Update Kaniko executor to 1.6.0
* Add support for separate control plane listener (disabled by default, available via the `ControlPlaneListener` feature gate)
* Support for Dual Stack networking

### Changes, deprecations and removals

* Strimzi API versions `v1alpha1` and `v1beta1` were removed from all Strimzi custom resources apart from `KafkaTopic` and `KafkaUser` (use `v1beta2` versions instead)
* The following annotations have been removed and cannot be used anymore:
  * `cluster.operator.strimzi.io/delete-claim` (used internally only - replaced by `strimzi.io/delete-claim`)
  * `operator.strimzi.io/generation` (used internally only - replaced by `strimzi.io/generation`)
  * `operator.strimzi.io/delete-pod-and-pvc` (use `strimzi.io/delete-pod-and-pvc` instead)
  * `operator.strimzi.io/manual-rolling-update` (use `strimzi.io/manual-rolling-update` instead)
* When the `class` field is configured in the `configuration` section of an Ingress-type listener, Strimzi will not automatically set the deprecated `kubernetes.io/ingress.class` annotation anymore. In case you still need this annotation, you can set it manually in the listener configuration using the [`annotations` field](https://strimzi.io/docs/operators/latest/full/using.html#property-listener-config-annotations-reference) or in the [`.spec.kafka.template` section](https://strimzi.io/docs/operators/latest/full/using.html#type-KafkaClusterTemplate-reference).
* The `.spec.kafkaExporter.template.service` section in the `Kafka` custom resource has been deprecated and will be removed in the next API version (the service itself was removed several releases ago).

## 0.22.0

* Add `v1beta2` version for all resources. `v1beta2` removes all deprecated fields.
* Add annotations that enable the operator to restart Kafka Connect connectors or tasks. The annotations can be applied to the KafkaConnector and the KafkaMirrorMaker2 custom resources.
* Add additional configuration options for the Kaniko executor used by the Kafka Connect Build on Kubernetes
* Add support for JMX options configuration of all Kafka Connect (KC, KC2SI, MM2)
* Update Strimzi Kafka OAuth to version 0.7 and add support for new features:
  * OAuth authentication over SASL PLAIN mechanism
  * Checking token audience
  * Validating tokens using JSONPath filter queries to perform custom checks
* Fix Cruise Control crash loop when updating container configurations
* Configure external logging `ConfigMap` name and key.
* Add support for configuring labels and annotations in ClusterRoleBindings created as part of Kafka and Kafka Connect clusters
* Add support for Ingress v1 in Kubernetes 1.19 and newer
* Add support for Kafka 2.6.1
* List topics used by a Kafka Connect connector in the `.status` section of the `KafkaConnector` custom resource
* Bump Cruise Control to v2.5.37 for Kafka 2.7 support. Note this new version of Cruise Control uses `Log4j 2` and is supported by dynamic logging configuration (where logging properties are defined in a ConfigMap). However, existing `Log4j` configurations must be updated to `Log4j 2` configurations.
* Support pausing reconciliation of CR with annotation `strimzi.io/pause-reconciliation`

### Changes, deprecations and removals

* In the past, when no Ingress class was specified in the Ingress-type listener in the Kafka custom resource, the 
  `kubernetes.io/ingress.class` annotation was automatically set to `nginx`. Because of the support for the new 
  IngressClass resource and the new `ingressClassName` field in the Ingress resource, the default value will not be set 
  anymore. Please use the `class` field in `.spec.kafka.listeners[].configuration` to specify the class name.
* The `KafkaConnectS2I` custom resource is deprecated and will be removed in the future. You can use the new [`KafkaConnect` build feature](https://strimzi.io/docs/operators/latest/full/deploying.html#creating-new-image-using-kafka-connect-build-str) instead.
* Removed support for Helm2 charts as that version is now unsupported. There is no longer the need for separate `helm2` and `helm3` binaries, only `helm` (version 3) is required.
* The following annotations are deprecated for a long time and will be removed in 0.23.0:
  * `cluster.operator.strimzi.io/delete-claim` (used internally only - replaced by `strimzi.io/delete-claim`)
  * `operator.strimzi.io/generation` (used internally only - replaced by `strimzi.io/generation`)
  * `operator.strimzi.io/delete-pod-and-pvc` (use `strimzi.io/delete-pod-and-pvc` instead)
  * `operator.strimzi.io/manual-rolling-update` (use `strimzi.io/manual-rolling-update` instead)
* External logging configuration has changed. `spec.logging.name` is deprecated. Moved to `spec.logging.valueFrom.configMapKeyRef.name`. Key in the `ConfigMap` is configurable via `spec.logging.valueFrom.configMapKeyRef.key`.
  * from
  ```
  logging:
    type: external
    name: my-config-map
  ```
  * to
  ```
  logging:
    type: external
    valueFrom:
      configMapKeyRef:
        name: my-config-map
        key: my-key
  ``` 
* Existing Cruise Control logging configurations must be updated from `Log4j` syntax to `Log4j 2` syntax.
  * For existing inline configurations, replace the `cruisecontrol.root.logger` property with `rootLogger.level`.
  * For existing external configurations, replace the existing configuration with a new configuration file named `log4j2.properties` using `log4j 2` syntax.

## 0.21.0

* Add support for declarative management of connector plugins in Kafka Connect CR 
* Add `inter.broker.protocol.version` to the default configuration in example YAMLs
* Add support for `secretPrefix` property for User Operator to prefix all secret names created from KafkaUser resource.
* Allow configuring labels and annotations for Cluster CA certificate secrets
* Add the JAAS configuration string in the sasl.jaas.config property to the generated secrets for KafkaUser with SCRAM-SHA-512 authentication.
* Strimzi `test-container` has been renamed to `strimzi-test-container` to make the name more clear
* Updated the CPU usage metric in the Kafka, ZooKeeper and Cruise Control dashboards to include the CPU kernel time (other than the current user time)
* Allow disabling ownerReference on CA secrets
* Make it possible to run Strimzi operators and operands with read-only root filesystem
* Move from Docker Hub to Quay.io as our container registry
* Add possibility to configure DeploymentStrategy for Kafka Connect, Kafka Mirror Maker (1 and 2), and Kafka Bridge
* Support passing metrics configuration as an external ConfigMap
* Enable CORS configuration for Cruise Control
* Add support for rolling individual Kafka or ZooKeeper pods through the Cluster Operator using an annotation
* Add support for Topology Spread Constraints in Pod templates
* Make Kafka `cluster-id` (KIP-78) available on Kafka CRD status
* Add support for Kafka 2.7.0

### Deprecations and removals
* The `metrics` field in the Strimzi custom resources has been deprecated and will be removed in the future. For configuring metrics, use the new `metricsConfig` field and pass the configuration via ConfigMap.

## 0.20.0

**Note: This is the last version of Strimzi that will support Kubernetes 1.11 and higher. Future versions will drop support for Kubernetes 1.11-1.15 and support only Kubernetes 1.16 and higher.**

* Add support for Kafka 2.5.1 and 2.6.0. Remove support for 2.4.0 and 2.4.1
* Remove TLS sidecars from Kafka pods => Kafka now uses native TLS to connect to ZooKeeper
* Updated to Cruise Control 2.5.11, which adds Kafka 2.6.0 support and fixes a previous issue with CPU utilization statistics for containers. As a result, the CpuCapacityGoal has now been enabled.
* Cruise Control metrics integration:
  * Enable metrics JMX exporter configuration in the `cruiseControl` property of the Kafka custom resource
  * New Grafana dashboard for the Cruise Control metrics
* Configure Cluster Operator logging using ConfigMap instead of environment variable and support dynamic changes  
* Switch to use the `AclAuthorizer` class for the `simple` Kafka authorization type. `AclAuthorizer` contains new features such as the ability to control the amount of authorization logs in the broker logs.
* Support dynamically changeable logging configuration of Kafka Connect and Kafka Connect S2I
* Support dynamically changeable logging configuration of Kafka brokers
* Support dynamically changeable logging configuration of Kafka MirrorMaker2
* Add support for `client.rack` property for Kafka Connect to use `fetch from closest replica` feature. 
* Refactored operators Grafana dashboard
  * Fixed bug on maximum reconcile time graph
  * Removed the avarage reconcile time graph
  * Rearranged graphs
* Make `listeners` configurable as an array and add support for more different listeners in single cluster
* Add support for configuring `hostAliases` in Pod templates
* Add new resource state metric in the operators for reflecting the reconcile result on a specific resource
* Add improvements for `oauth` authentication, and `keycloak` authorization:
  * Support for re-authentication was added, which also enforces access token lifespan on the Kafka client session
  * Permission changes through Keycloak Authorization Services are now detected by Kafka Brokers

### Deprecations and removals

#### Redesign of the `.spec.kafka.listeners` section

The `.spec.kafka.listeners` section of the Kafka CRD has been redesigned to allow configuring more different listeners.
The old `listeners` object which allowed only configuration of one`plain`, one `tls`, and one `external` listener is now deprecated and will be removed in the future.
It is replaced with an array allowing configuration of multiple different listeners:

```yaml
listeners:
  - name: local
    port: 9092
    type: internal
    tls: true
  - name: external1
    port: 9093
    type: loadbalancer
    tls: true
  - name: external2
    port: 9094
    type: nodeport
    tls: true
```

This change includes some other changes:
* The `tls` field is now required.
* The former `overrides` section is now merged with the `configuration` section.
* The `dnsAnnotations` field has been renamed to `annotations` since we found out it has wider use.
* Configuration of `loadBalancerSourceRanges` and `externalTrafficPolicy` has been moved into listener configuration. Its use in the `template` section is now deprecated.
* For `type: internal` listeners, you can now use the flag `useServiceDnsDomain` to define whether they should use the fully qualified DNS names including the cluster service suffix (usually `.cluster.local`). This option defaults to false.
* All listeners now support configuring the advertised hostname and port.
* `preferredAddressType` has been removed to `preferredNodePortAddressType`.

To convert the old format into the new format with backwards compatibility, you should use following names and types:
* For the old `plain` listener, use the name `plain`, port `9092` and type `internal`.
* For the old `tls` listener, use the name `tls`, port `9093` and type `internal`.
* For the old `external` listener, use the name `external`, port `9094`.

For example the following old configuration:

```yaml
listeners:
  plain:
    # ...
  tls: 
    # ...
  external:
    type: loadbalancer 
    # ...
```

Will look like this in the new format:

```yaml
listeners:
  - name: plain
    port: 9092
    type: internal
    tls: false
  - name: tls
    port: 9093
    type: internal
    tls: true
  - name: external
    port: 9094
    type: loadbalancer
    tls: true
```

#### Removal of monitoring port on Kafka and ZooKeeper related services

The `PodMonitor` resource is now used instead of the `ServiceMonitor` for scraping metrics from Kafka, ZooKeeper, Kafka Connect and so on.
For this reason, we have removed the monitoring port `tcp-prometheus` (9404) on all the services where it is declared (Kafka bootstrap, ZooKeeper client and so on).
It was already deprecated in the previous 0.19.0 release.
Together with it we have also removed the Prometheus annotations from the services. If you want to add them, you can use the templates.
See here https://strimzi.io/docs/operators/master/using.html#assembly-customizing-kubernetes-resources-str for more details about templates usage.
Finally, the Kafka Exporter service was has been removed because it was used just for the monitoring port.

#### Deprecation of Kafka TLS sidecar configuration

Since the Kafka TLS sidecar has been removed, the related configuration options in the Kafka custom resource are now deprecated:
* `.spec.kafka.tlsSidecar`
* `.spec.kafka.template.tlsSidecar`

## 0.19.0

* Add support for authorization using Open Policy Agent
* Add support for scale subresource to make scaling of following resources easier:
  * KafkaConnect
  * KafkaConnectS2I
  * KafkaBridge
  * KafkaMirrorMaker
  * KafkaMirrorMaker2
  * KafkaConnector 
* Remove deprecated `Kafka.spec.topicOperator` classes and deployment logic
* Use Java 11 as the Java runtime
* Removed the need to manually create Cruise Control metrics topics if topic auto creation is disabled.
* Migration to Helm 3
* Refactored the format of the `KafkaRebalance` resource's status. The state of the rebalance is now displayed in the associated `Condition`'s `type` field rather than the `status` field. This was done so that the information would display correctly in various Kubernetes tools.
* Added performance tuning options to the `KafkaRebalance` CR and the ability to define a regular expression that will exclude matching topics from a rebalance optimization proposal.
* Use Strimzi Kafka Bridge 0.18.0
* Make it possible to configure labels and annotations for secrets created by the User Operator
* Strimzi Kafka Bridge metrics integration:
  * enable/disable metrics in the KafkaBridge custom resource
  * new Grafana dashboard for the bridge metrics
* Support dynamically changeable logging in the Entity Operator and Kafka Bridge 
* Extended the Grafana example dashboard for Kafka Connect to provide more relevant information

### Deprecations and removals

#### Deprecation of Helm v2 chart

The Helm v2 support will end soon. 
Bug fixing should stop on August 13th 2020 and security fixes on November 13th.
See https://helm.sh/blog/covid-19-extending-helm-v2-bug-fixes/ for more details.

In sync with that, the Helm v2 chart of Strimzi Cluster Operator is now deprecated and will be removed in the future as Helm v2 support ends.
Since Strimzi 0.19.0, we have a new chart for Helm v3 which can be used instead.

#### Removal of v1alpha1 versions of several custom resources

In Strimzi 0.12.0, the `v1alpha1` versions of the following resources have been deprecated and replaced by `v1beta1`:
* `Kafka`
* `KafkaConnect`
* `KafkaConnectS2I`
* `KafkaMirrorMaker`
* `KafkaTopic`
* `KafkaUser`

In the next release, the `v1alpha1` versions of these resources will be removed. 
Please follow the guide for upgrading the resources: https://strimzi.io/docs/operators/latest/full/deploying.html#assembly-upgrade-resources-str.

#### Removal deprecated cadvisor metric labels

The `pod_name` and `container_name` labels provided on the cadvisor metrics are now just `pod` and `container` starting from Kubernetes 1.16.
We removed the old ones from the Prometheus scraping configuration/alerts and on the Kafka and ZooKeeper dashboard as well.
It means that the charts related to memory and CPU usage are not going to work on Kuvbernetes version previous 1.14.
For more information on what is changed: https://github.com/strimzi/strimzi-kafka-operator/pull/3312

#### Deprecation of monitoring port on Kafka and ZooKeeper related services

The `PodMonitor` resource is now used instead of the `ServiceMonitor` for scraping metrics from Kafka, ZooKeeper, Kafka Connect and so on.
For this reason, we are deprecating the monitoring port `tcp-prometheus` (9404) on all the services where it is declared (Kafka bootstrap, ZooKeeper client and so on).
This port will be removed in the next release.
Together with it we will also remove the Prometheus annotation from the service.

#### Removal warning of Cluster Operator log level

Because of the new Cluster Operator dynamic logging configuration via [PR#3328](https://github.com/strimzi/strimzi-kafka-operator/pull/3328) we are going to remove the `STRIMZI_LOG_LEVEL` environment variable from the Cluster Operator deployment YAML file in the 0.20.0 release.

## 0.18.0

* Add possibility to set Java System Properties for User Operator and Topic Operator via `Kafka` CR.
* Make it possible to configure PodManagementPolicy for StatefulSets
* Update build system to use `yq` version 3 (https://github.com/mikefarah/yq)
* Add more metrics to Cluster and User Operators
* New Grafana dashboard for Operator monitoring 
* Allow `ssl.cipher.suites`, `ssl.protocol` and `ssl.enabled.protocols` to be configurable for Kafka and the different components supported by Strimzi
* Add support for user configurable SecurityContext for each Strimzi container
* Allow standalone User Operator to modify status on KafkaUser
* Add support for Kafka 2.4.1
* Add support for Kafka 2.5.0
* Remove TLS sidecars from ZooKeeper pods, using native ZooKeeper TLS support instead
* Add metrics for Topic Operator
* Use Strimzi Kafka Bridge 0.16.0
* Add support for CORS in the HTTP Kafka Bridge
* Pass HTTP Proxy configuration from operator to operands
* Add Cruise Control support, KafkaRebalance resource and rebalance operator

## 0.17.0

* Add possibility to set Java System Properties via CR yaml
* Add support for Mirror Maker 2.0
* Add Jmxtrans deployment
* Add public keys of TLS listeners to the status section of the Kafka CR
* Add support for using a Kafka authorizer backed by Keycloak Authorization Services

## 0.16.0

* Add support for Kafka 2.4.0 and upgrade from Zookeeper 3.4.x to 3.5.x
* Drop support for Kafka 2.2.1 and 2.3.0
* Add KafkaConnector resource and connector operator
* Let user choose which node address will be used as advertised host (`ExternalDNS`, `ExternalIP`, `InternalDNS`, `InternalIP` or `Hostname`)
* Add support for tini
* When not explicitly configured by the user in `jvmOptions`, `-Xmx` option is calculated from memory requests rather than from memory limits
* Expose JMX port on Kafka brokers via an internal service
* Add support for `externalTrafficPolicy` and `loadBalancerSourceRanges` properties on loadbalancer and nodeport type services
* Add support for user quotas
* Add support for Istio protocol selection in service port names  
Note: Strimzi is essentially adding a `tcp-` prefix to the port names in Kafka services and headless services.  
(e.g clientstls -> tcp-clientstls)
* Add service discovery labels and annotations
* Add possibility to specify custom server certificates to TLS based listeners

## 0.15.0

* Drop support for Kafka 2.1.0, 2.1.1, and 2.2.0
* Add support for Kafka 2.3.1
* Improved Kafka rolling update
* Improve Kafka Exporter Grafana dashboard
* Add sizeLimit option to ephemeral storage (#1505)
* Add `schedulerName` to `podTemplate` (#2114)
* Allow overriding the auto-detected Kubernetes version
* Garbage Collection (GC) logging disabled by default
* Providing PKCS12 truststore and password in the cluster and clients CA certificates Secrets
* Providing PKCS12 keystore and password in the TLS based KafkaUser related Secret

## 0.14.0

* Add support for configuring Ingress class (#1716)
* Add support for setting custom environment variables in all containers
* Add liveness and readiness checks to Mirror Maker
* Allow configuring loadBalancerIP for LoadBalancer type services
* Allow setting labels and annotations for Persistent Volume Claims
* Add support for Jaeger tracing in Kafka Mirror Maker and Kafka Connect
* Add support for deploying Kafka Exporter
* Add initial support for OAuth authentication

## 0.13.0

* Allow users to manually configure ACL rules (for example, using `kafka-acls.sh`) for special Kafka users `*` and `ANONYMOUS` without them being deleted by the User Operator
* Add support for configuring a Priority Class name for Pods deployed by Strimzi
* Add support for Kafka 2.3.0
* Add support for Kafka User resource status
* Add support for Kafka Connect resource status
* Add support for Kafka Connect S2I resource status
* Add support for Kafka Bridge resource status
* Add support for Kafka Mirror Maker resource status
* Add support for DNS annotations to `nodeport` type external listeners

## 0.12.0

* **Drop support for Kubernetes 1.9 and 1.10 and OpenShift 3.9 and 3.10.**
**Versions supported since Strimzi 0.12.0 are Kubernetes 1.11 and higher and OpenShift 3.11 and higher.** 
**This was required because the CRD versioning and CRD subresources support.** 
* Added support for Kafka 2.2.0 and 2.1.1, dropped support for Kafka 2.0.0 and 2.0.1
* Persistent storage improvements
  * Add resizing of persistent volumes
  * Allow to specify different storage class for every broker
  * Adding and removing volumes in Jbod Storage
* Custom Resources improvements
  * New CRD version `v1beta1`. See documentation for more information about upgrading from `v1alpha1` to `v1beta1`.
  * Log at the warn level when a custom resource uses deprecated or unknown properties
  * Add initial support for the `status` sub-resource in the `Kafka` custom resource 
* Add support for [Strimzi Kafka Bridge](https://github.com/strimzi/strimzi-kafka-bridge) for HTTP protocol
* Reduce the number of container images needed to run Strimzi to just two: `kafka` and `operator`.
* Add support for unprivileged users to install the operator with Helm
* Support experimental off-cluster access using Kubernetes Nginx Ingress
* Add ability to configure Image Pull Secrets for all pods in Cluster Operator
* Support for SASL PLAIN mechanism in Kafka Connect and Mirror Maker (for use with non-Strimzi Kafka cluster)

## 0.11.0

* Add support for JBOD storage for Kafka brokers
* Allow users to configure the default ImagePullPolicy
* Add Prometheus alerting
    * Resources for alert manager deployment and configuration
    * Alerting rules with alert examples from Kafka and Zookeeper metrics
* Enrich configuration options for off cluster access
* Support for watching all namespaces
* Operator Lifecycle Manager integration

## 0.10.0

* Support for Kafka 2.1.0
* Support for Kafka upgrades
* Add healthchecks to TLS sidecars
* Add support for new fields in the Pod template: terminationGracePeriod, securityContext and imagePullSecrets
* Rename annotations to use the `strimzi.io` domain consistently (The old annotations are deprecated, but still functional):
    * `cluster.operator.strimzi.io/delete-claim` → `strimzi.io/delete-claim` 
    * `operator.strimzi.io/manual-rolling-update` → `strimzi.io/manual-rolling-update` 
    * `operator.strimzi.io/delete-pod-and-pvc` → `strimzi.io/delete-pod-and-pvc`
    * `operator.strimzi.io/generation` → `strimzi.io/generation`
* Add support for mounting Secrets and Config Maps into Kafka Connect and Kafka Connect S2I
* Add support for NetworkPolicy peers in listener configurations
* Make sure the TLS sidecar pods shutdown only after the main container
* Add support for Pod Disruption Budgets

## 0.9.0

* Add possibility to label and annotate different resources (#1039)
* Add support for TransactionalID in KafkaUser resource
* Update to Kafka 2.0.1
* Add maintenance time windows support for allowing CA certificates renewal rolling update started only in specific times (#1117)  
* Add support for upgrading between Kafka versions (#1103). This removes support for `STRIMZI_DEFAULT_KAFKA_IMAGE` environment variable in the Cluster Operator, replacing it with `STRIMZI_KAFKA_IMAGES`.  


## 0.8.2

* Run images under group 0 to avoid storage issues

## 0.8.1

* Fix certificate renewal issues

## 0.8.0

* Support for unencrypted connections on LoadBalancers and NodePorts.
* Better support for TLS hostname verification for external connections
* Certificate renewal / expiration
* Mirror Maker operator
* Triggering rolling update / pod deletion manually

## 0.7.0

* Exposing Kafka to the outside using:
  * OpenShift Routes
  * LoadBalancers
  * NodePorts
* Use less wide RBAC permissions (`ClusterRoleBindings` where converted to `RoleBindings` where possible)
* Support for SASL authentication using the SCRAM-SHA-512 mechanism added to Kafka Connect and Kafka Connect with S2I support 
* Network policies for managing access to Zookeeper ports and Kafka replication ports
* Use OwnerReference and Kubernetes garbage collection feature to delete resources and to track the ownership

## 0.6.0

* Helm chart for Strimzi Cluster Operator
* Topic Operator moving to Custom Resources instead of Config Maps
* Make it possible to enabled and disable:
  * Listeners
  * Authorization
  * Authentication
* Configure Kafka _super users_ (`super.users` field in Kafka configuration)
* User Operator
  * Managing users and their ACL rights
* Added new Entity Operator for deploying:
  * User Operator
  * Topic Operator
* Deploying the Topic Operator outside of the new Entity Operator is now deprecated
* Kafka 2.0.0
* Kafka Connect:
  * Added TLS support for connecting to the Kafka cluster
  * Added TLS client authentication when connecting to the Kafka cluster 

## 0.5.0

* The Cluster Operator now manages RBAC resource for managed resources:
    * `ServiceAccount` and `ClusterRoleBindings` for Kafka pods
    * `ServiceAccount` and `RoleBindings` for the Topic Operator pods
* Renaming of Kubernetes services (Backwards incompatible!)
  * Kubernetes services for Kafka, Kafka Connect and Zookeeper have been renamed to better correspond to their purpose
  * `xxx-kafka` -> `xxx-kafka-bootstrap`
  * `xxx-kafka-headless` -> `xxx-kafka-brokers`
  * `xxx-zookeeper` -> `xxx-zookeeper-client`
  * `xxx-zookeeper-headless` -> `xxx-zookeeper-nodes`
  * `xxx-connect` -> `xxx-connect-api`
* Cluster Operator moving to Custom Resources instead of Config Maps
* TLS support has been added to Kafka, Zookeeper and Topic Operator. The following channels are now encrypted:
    * Zookeeper cluster communication
    * Kafka cluster commbunication
    * Communication between Kafka and Zookeeper
    * Communication between Topic Operator and Kafka / Zookeeper
* Logging configuration for Kafka, Kafka Connect and Zookeeper
* Add support for [Pod Affinity and Anti-Affinity](https://kubernetes.io/docs/concepts/configuration/assign-pod-node/#affinity-and-anti-affinity)
* Add support for [Tolerations](https://v1-9.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.9/#toleration-v1-core)
* Configuring different JVM options
* Support for broker rack in Kafka

## 0.4.0

* Better configurability of Kafka, Kafka Connect, Zookeeper
* Support for Kubernetes request and limits
* Support for JVM memory configuration of all components
* Controllers renamed to operators
* Improved log verbosity of Cluster Operator
* Update to Kafka 1.1.0
