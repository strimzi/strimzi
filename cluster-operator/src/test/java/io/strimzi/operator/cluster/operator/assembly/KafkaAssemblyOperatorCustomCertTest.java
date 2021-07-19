/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.function.Function;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaList;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.certs.CertManager;
import io.strimzi.operator.KubernetesVersion;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.FeatureGates;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.ZookeeperLeaderFinder;
import io.strimzi.operator.cluster.operator.resource.ZookeeperScalerProvider;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.MetricsProvider;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.test.mockkube.MockKube;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;

@ExtendWith(VertxExtension.class)
public class KafkaAssemblyOperatorCustomCertTest {
    private final KubernetesVersion kubernetesVersion = KubernetesVersion.V1_18;
    private final MockCertManager certManager = new MockCertManager();
    private final PasswordGenerator passwordGenerator = new PasswordGenerator(10, "a", "a");
    private final ClusterOperatorConfig config = ResourceUtils.dummyClusterOperatorConfig(VERSIONS);
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private final String namespace = "testns";
    private final String clusterName = "testkafka";
    protected static Vertx vertx;

    private Kafka kafka;
    private KafkaAssemblyOperator operator;
    private KubernetesClient client;
    private final List<Function<Pod, List<String>>> functionArgumentCaptor = new ArrayList<>();

    /**
     * Mock the KafkaAssemblyOperator and override reconcile to only run through the steps we want to test
     */
    class MockKafkaAssemblyOperator extends KafkaAssemblyOperator  {

        public MockKafkaAssemblyOperator(Vertx vertx, PlatformFeaturesAvailability pfa, CertManager certManager, PasswordGenerator passwordGenerator, ResourceOperatorSupplier supplier, ClusterOperatorConfig config) {
            super(vertx, pfa, certManager, passwordGenerator, supplier, config);
        }

        @Override
        ReconciliationState createReconciliationState(Reconciliation reconciliation, Kafka kafkaAssembly) {
            return new ReconciliationState(reconciliation, kafkaAssembly) {
                @Override
                Future<Void> maybeRollKafka(StatefulSet sts, Function<Pod, List<String>> podNeedsRestart) {
                    functionArgumentCaptor.add(podNeedsRestart);
                    return Future.succeededFuture();
                }
            };
        }

        @Override
        Future<Void> reconcile(ReconciliationState reconcileState)  {
            return reconcileState.reconcileCas(this::dateSupplier)
                    .compose(state -> state.getKafkaClusterDescription())
                    .compose(state -> state.customListenerCertificates())
                    .compose(state -> state.kafkaStatefulSet())
                    .compose(state -> state.kafkaRollingUpdate())
                    .map((Void) null);
        }
    }

    @BeforeAll
    public static void before() {
        vertx = Vertx.vertx();
    }

    @BeforeEach
    public void setup() {
        kafka = createKafka();

        client = new MockKube()
                .withCustomResourceDefinition(Crds.kafka(), Kafka.class, KafkaList.class).end()
                .build();
        Crds.kafkaOperation(client).inNamespace(namespace).create(kafka);
        client.secrets().inNamespace(namespace).create(getTlsSecret());
        client.secrets().inNamespace(namespace).create(getExternalSecret());
        Secret secret = new SecretBuilder()
            .withNewMetadata()
                .withNamespace(namespace)
                .withName("testkafka-cluster-operator-certs")
            .endMetadata()
            .addToData("foo", "bar")
            .build();
        client.secrets().inNamespace(namespace).create(secret);
        ResourceOperatorSupplier supplier = new ResourceOperatorSupplier(vertx, client, mock(ZookeeperLeaderFinder.class),
                mock(AdminClientProvider.class), mock(ZookeeperScalerProvider.class),
                mock(MetricsProvider.class), new PlatformFeaturesAvailability(false, KubernetesVersion.V1_20), FeatureGates.NONE, 10000);
        operator = new MockKafkaAssemblyOperator(vertx, new PlatformFeaturesAvailability(false, kubernetesVersion),
                certManager,
                passwordGenerator,
                supplier,
                config);

    }

    @AfterAll
    public static void after() {
        vertx.close();
    }

    public Kafka createKafka() {
        return new KafkaBuilder()
                .withNewMetadata()
                    .withName(clusterName)
                    .withNamespace(namespace)
                    .withGeneration(2L)
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(3)
                        .withListeners(new GenericKafkaListenerBuilder()
                                    .withName("tls")
                                    .withPort(9093)
                                    .withType(KafkaListenerType.INTERNAL)
                                    .withTls(true)
                                    .withNewConfiguration()
                                        .withNewBrokerCertChainAndKey()
                                            .withSecretName("my-tls-secret")
                                            .withCertificate("tls.crt")
                                            .withKey("tls.key")
                                        .endBrokerCertChainAndKey()
                                    .endConfiguration()
                                    .build(),
                                new GenericKafkaListenerBuilder()
                                    .withName("external")
                                    .withPort(9094)
                                    .withType(KafkaListenerType.NODEPORT)
                                    .withTls(true)
                                    .withNewConfiguration()
                                        .withNewBrokerCertChainAndKey()
                                            .withSecretName("my-external-secret")
                                            .withCertificate("tls.crt")
                                            .withKey("tls.key")
                                        .endBrokerCertChainAndKey()
                                    .endConfiguration()
                                    .build())
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endKafka()
                    .withNewZookeeper()
                        .withReplicas(3)
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endZookeeper()
                .endSpec()
                .build();
    }

    public Secret getTlsSecret() {
        return new SecretBuilder()
                .withNewMetadata()
                    .withName("my-tls-secret")
                .endMetadata()
                .addToData("tls.crt", "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUVVekNDQXp1Z0F3SUJBZ0lVZWlqTU02TVBIazB3R24xV3FtL3o4ZTNkYUpJd0RRWUpLb1pJaHZjTkFRRUwKQlFBd1ZERUxNQWtHQTFVRUJoTUNRMW94RHpBTkJnTlZCQWNUQmxCeVlXZDFaVEViTUJrR0ExVUVDaE1TU21GcgpkV0lnVTJOb2IyeDZMQ0JKYm1NdU1SY3dGUVlEVlFRREV3NUpiblJsY20xbFpHbGhkR1ZEUVRBZUZ3MHhPVEV5Ck16RXhPVEl4TURCYUZ3MHlNREV4TWprd016SXhNREJhTUU0eEN6QUpCZ05WQkFZVEFrTmFNUTh3RFFZRFZRUUgKRXdaUWNtRm5kV1V4R3pBWkJnTlZCQW9URWtwaGEzVmlJRk5qYUc5c2Vpd2dTVzVqTGpFUk1BOEdBMVVFQXhNSQpTVzUwWlhKdVlXd3dnZ0VpTUEwR0NTcUdTSWIzRFFFQkFRVUFBNElCRHdBd2dnRUtBb0lCQVFEY0hYS3lzRlc2CnZlaU85VjVhMkxjZVREY0I4eWVuV1hMdmQ0U0ZYZHQwZU9JTVZnQUdhSVhVc2x4V3ArOVBUUjZGSlpDNzFlbzMKVVBoeERxTTFxbDdXVW5iUXNWbGx5OVlzZ3J5UDE2TjJ3eFJRN3FSc29MbVVwTkRZN2pNOU5sT2JzTVQwaDFKcgozRmhYT0JsL2Z5WWJVaVpnZC9tWGNWMTFhTkMrOFkrQzVTekVNZWt4YkhFbGtSQ2RQekhZUWpqM0EwaDZnektZCi9lNElCWm5kRWs2SmV6MXlmY00vRy8vcmN0UWFSMTB1OXBxRVdwbzlOQllBdWdhTUpGZm51QnFENlFySEN1bGMKQm1mNlQ2allwakdhL0ovQzZ1NkZXcFBOK1VtZXl4L3hPQWVWekxnWlV6ZTZxZlFTenI1NFF4MElZNitrUWJoRQpGTVRDQUlaNFQ0WEJBZ01CQUFHamdnRWhNSUlCSFRBT0JnTlZIUThCQWY4RUJBTUNCYUF3SFFZRFZSMGxCQll3CkZBWUlLd1lCQlFVSEF3RUdDQ3NHQVFVRkJ3TUNNQXdHQTFVZEV3RUIvd1FDTUFBd0hRWURWUjBPQkJZRUZFSkUKU08xMFUvVzVtVW0rVFRWWmF5bUc1SGFpTUI4R0ExVWRJd1FZTUJhQUZPa0c5U0EyT245Y3ZHZHg4ajJBQW9GWgp4Rmc1TUlHZEJnTlZIUkVFZ1pVd2daS0NEeW91Ylhsd2NtOXFaV04wTG5OMlk0SW9LaTV0ZVMxamJIVnpkR1Z5CkxXdGhabXRoTFdKeWIydGxjbk11Ylhsd2NtOXFaV04wTG5OMlk0SWRLaTV0ZVhCeWIycGxZM1F1YzNaakxtTnMKZFhOMFpYSXViRzlqWVd5Q05pb3ViWGt0WTJ4MWMzUmxjaTFyWVdacllTMWljbTlyWlhKekxtMTVjSEp2YW1WagpkQzV6ZG1NdVkyeDFjM1JsY2k1c2IyTmhiREFOQmdrcWhraUc5dzBCQVFzRkFBT0NBUUVBWHRVSXpLeTRPY1IwClBQaE51c0tmS2UrcTdBZG1paUhudnd1djJsMU0vNTZsbDFDbWtTWk9jUlNpZHBTZ2FqeldtcGJDckoydTNRV3oKTUhDemxhN3BwQnNLT1p0NDRsVGMzSGlEZk5HaWF4NGJHWXdVMTQzc2p3VkRPYm5xK2RBdUtjcklpTU80YWpPQQp3OXdpOVNoVnkzOHRDSHo0MU9uYkYrODRwU1k2NzdWMFJzKzI5dFpVdk9kTkk0R2xmL0hJWDJkZjlCaUQ4TXhLCnArWVJrbHJXQjJQU1Zib1p5bklDV3lkNzZXVkhDdU5GNlNFVk9sNERKTkV3dHphb0tDU1RPT2JlUmxFZEwrMU8KRW9IbFFjTWlrYjUxbWRVRFhVYnoySG80U2ZQTjN2MlNDcmFmb3VxMHUxcmpvOHJtVEw1UFBvNitlMllPdGU5VgpUdTVxbS9vNVhRPT0KLS0tLS1FTkQgQ0VSVElGSUNBVEUtLS0tLQotLS0tLUJFR0lOIENFUlRJRklDQVRFLS0tLS0KTUlJRGtUQ0NBbm1nQXdJQkFnSVVNU0Z6WEdCYnNtdVcxY3VaYU9CeUsyK01LK1V3RFFZSktvWklodmNOQVFFTApCUUF3VERFTE1Ba0dBMVVFQmhNQ1Exb3hEekFOQmdOVkJBY1RCbEJ5WVdkMVpURWJNQmtHQTFVRUNoTVNTbUZyCmRXSWdVMk5vYjJ4NkxDQkpibU11TVE4d0RRWURWUVFERXdaU2IyOTBRMEV3SGhjTk1Ua3hNak14TVRreU1UQXcKV2hjTk1qQXhNVEk1TURNeU1UQXdXakJVTVFzd0NRWURWUVFHRXdKRFdqRVBNQTBHQTFVRUJ4TUdVSEpoWjNWbApNUnN3R1FZRFZRUUtFeEpLWVd0MVlpQlRZMmh2Ykhvc0lFbHVZeTR4RnpBVkJnTlZCQU1URGtsdWRHVnliV1ZrCmFXRjBaVU5CTUlJQklqQU5CZ2txaGtpRzl3MEJBUUVGQUFPQ0FROEFNSUlCQ2dLQ0FRRUF2dW15TnFBaXp2VEIKQ0xRa3FiVDBEajI0R1ZwcVJsb0RSTGdRc0xzMFhHMCtPZVMrc0UyU2ZLTkhJeC9BK1pzUEFoTm04L1BlL01UKwpHNFBzYzgydHNpNitTZWJlYjBENExuOVI3UXVlWFpKTXlxaXhSWFlzcEZBMHA5bXhwc1NpZ0NnTFl3Y3NCVElkCm12U055VllzV2hUUHpuMXM4VUJ2SlVoenBCKzBLM1d6WkhYMEVJYVh3ZmtsM1Fob3JQZDdyQ0RUVXAzQlNwdWUKSXRENG1VcCtNV3NvSDZzRzUrazBIeUNISzVEUS9qN2xSb1Y2dGhsSkdJdkxXbmhodFRLNjVOQThsQk92Wkd6UQpQMVBaMUwreEZRUXZyZDJKMUltczZicmM4NytqM0JEZ0VxZ1YvSjJGYmtEL3JQSHVFTDRSVndqS3l2YU1Tc2crCkthU3FjQ3VJR1FJREFRQUJvMk13WVRBT0JnTlZIUThCQWY4RUJBTUNBZ1F3RHdZRFZSMFRBUUgvQkFVd0F3RUIKL3pBZEJnTlZIUTRFRmdRVTZRYjFJRFk2ZjF5OFozSHlQWUFDZ1ZuRVdEa3dId1lEVlIwakJCZ3dGb0FVSzZhZApWaHk5bmtBR1JGbXorU3MyQkNvVUhua3dEUVlKS29aSWh2Y05BUUVMQlFBRGdnRUJBTHRKRjJTY3NkVUNzT2VKCmU0N2grbG5UNHRJU2s3WVVNQk02Rlc1bFhPU05PRXhrVEs5THBOd1hpeGFQVWlLZFo1RWhURE1KUDZuNkJZaVMKV01wRU9aNmVQY3p5bVZ5cHN3KzhZUXJ6U3ByMG1UL1l3L2pTQzRwTXNXL1dBNWYwWWpGMTVidGR2U01kekd5UAp5MjlEL1B5Vy9jQnRiNlhyZGtsKzRmZUY2a1Z6bWZwWDhsSklVRmhqK0ppZmNrRWdJTkhYTHZ1SjFXWWFUbkxpClZTWi9FVUQxK0pabzZaOElFMmRsd21OQXhQc0pCSnNiUFF0eUQ4SEg1clJtWW5LaXN5Q1dvU0xIUjJRZlA4SzYKOGFNMVpxTEkvWWxmditPMzlQQnZ4eEFTZldta2VzbHp1anBUYnZTV1hRNHk1dFEvRWhvSlFjQnVsOUhWc2xiRgpKSkhTRWtnPQotLS0tLUVORCBDRVJUSUZJQ0FURS0tLS0tCi0tLS0tQkVHSU4gQ0VSVElGSUNBVEUtLS0tLQpNSUlEYURDQ0FsQ2dBd0lCQWdJVWM1Sm1sYlEwQjJDcWcxWDV6MGs1emdyU0lVc3dEUVlKS29aSWh2Y05BUUVMCkJRQXdUREVMTUFrR0ExVUVCaE1DUTFveER6QU5CZ05WQkFjVEJsQnlZV2QxWlRFYk1Ca0dBMVVFQ2hNU1NtRnIKZFdJZ1UyTm9iMng2TENCSmJtTXVNUTh3RFFZRFZRUURFd1pTYjI5MFEwRXdIaGNOTVRreE1qTXhNVGt5TVRBdwpXaGNOTWpReE1qSTVNVGt5TVRBd1dqQk1NUXN3Q1FZRFZRUUdFd0pEV2pFUE1BMEdBMVVFQnhNR1VISmhaM1ZsCk1Sc3dHUVlEVlFRS0V4SktZV3QxWWlCVFkyaHZiSG9zSUVsdVl5NHhEekFOQmdOVkJBTVRCbEp2YjNSRFFUQ0MKQVNJd0RRWUpLb1pJaHZjTkFRRUJCUUFEZ2dFUEFEQ0NBUW9DZ2dFQkFNVDdlMDUvdmoyVm5IMFl0QXRMeGlQSgpaYkoyTzZRb25ldFRiNnltT0xaU0p2d0Uyd1RUQnlXNmxXWHZaVWsvNlNwRDQ5ODZ4eXM0RUs0bkc3WWUwOGx6Cjl4OVlZSUFhU0ptcEpmcjF2SkZBNnhCQWVZTDFqNEQ0T1kyUk80Qnp2Tmtobml3SmRVQXpzZCtVQzJzTW41SE4KZ2hTQTlzejNlTjVrcXAzNzNkdFBETWgyUVRZZnMvTFgySVhuSEEzeWhRRDZlZktxTEpZR2ZYTFZTdWNhNmYrawpUTkVBVmpDQ0E1bEl5OFJ1L25LTlZVQXlvTE5CSzI2R0prRTBuNU1qMzArRVhpdFE3YlN2SEUzdm1zRFFPTnl5CkF1K0dEbEl6WWtYOXpNUjRnYnNKNDQxK3dUWE5yVWtKRmVtb1B4c3dhcEFLc3FSTlljK3dXVUJPR21ZV2xHOEMKQXdFQUFhTkNNRUF3RGdZRFZSMFBBUUgvQkFRREFnRUdNQThHQTFVZEV3RUIvd1FGTUFNQkFmOHdIUVlEVlIwTwpCQllFRkN1bW5WWWN2WjVBQmtSWnMva3JOZ1FxRkI1NU1BMEdDU3FHU0liM0RRRUJDd1VBQTRJQkFRQXhqbEMrCm5lYnNndzlZSUs3NndzTWpFZ01BNlIySzVub25nVllKZWJHZXpnejBNaW5md0IxbjVnd2xGSkRZZHJvSUhmSmQKV3pxakpCdnhqUTlxUURoYzcyaGJkK3NlelNnUFZNR29Hb2l1cmJLY3VDa3lBalZMK1M4eFNHSkY5Ti81bEtwUQpqTklMZnBtSzlxMWZlam4zYzJmcFk0eE1aRnRUWk9qZVN6SGhLdTZ2VnVLZGZYYWRuQllWOTJPWXhUeXFJVk9CCmNPMm9EUDlvQmZjWlY4N0ZTSG9zY0dUOXRnd1F5R09zbEk3YmlObTFnRmZRL1VzcEhKRVltcy8za2NKRC9vOFkKS0lKeUFPUDNwYngzc0FhTWYrVHVaUkN6WGV5SFVUUzM1a3VoYjdvdEFTYmh1amlEaHRTeHFwL05CT3lBL2tmeApuQXN6SUdEMVdXYnBOSjMrCi0tLS0tRU5EIENFUlRJRklDQVRFLS0tLS0K")
                .addToData("tls.key", "LS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tCk1JSUV2Z0lCQURBTkJna3Foa2lHOXcwQkFRRUZBQVNDQktnd2dnU2tBZ0VBQW9JQkFRRGNIWEt5c0ZXNnZlaU8KOVY1YTJMY2VURGNCOHllbldYTHZkNFNGWGR0MGVPSU1WZ0FHYUlYVXNseFdwKzlQVFI2RkpaQzcxZW8zVVBoeApEcU0xcWw3V1VuYlFzVmxseTlZc2dyeVAxNk4yd3hSUTdxUnNvTG1VcE5EWTdqTTlObE9ic01UMGgxSnIzRmhYCk9CbC9meVliVWlaZ2QvbVhjVjExYU5DKzhZK0M1U3pFTWVreGJIRWxrUkNkUHpIWVFqajNBMGg2Z3pLWS9lNEkKQlpuZEVrNkplejF5ZmNNL0cvL3JjdFFhUjEwdTlwcUVXcG85TkJZQXVnYU1KRmZudUJxRDZRckhDdWxjQm1mNgpUNmpZcGpHYS9KL0M2dTZGV3BQTitVbWV5eC94T0FlVnpMZ1pVemU2cWZRU3pyNTRReDBJWTYra1FiaEVGTVRDCkFJWjRUNFhCQWdNQkFBRUNnZ0VCQU5JblAzb0JSMmlLcG4zUElLM2wyVGVSRnJmQzJxbzVpYlcrUzRVMXJqQU8KdGV1SE5IRHAzRlROa2NHZWhxb1UvRDJ0TnZsUGJGWXg5WEdqd3dtYXh2OGpMcE5qci9HejRxRU9sVTlVVjVvcworTG1vanMyenlsdHozSDR4TmpTTUtOa3R0VzJ0d1hCL3FNeGxJRnNOSDJuWVRoR3VtbHNQL21YNWs4dXFRQlY4ClNMQk13U1RodXlaZDgydEw1YTlhYTV0cHVpRkNpbm13QVF3OGRpMVZGaWwzbVdaZHpySnkxeWNzVWxYeUg0K2sKcVdRUVpUZFJ0bGVDTENTZi85VkFIMmt1SXVvcE1yZWxxeVArNWlDUlp0L2J1VUltNVV4RWNxbzVOcVJ3TlFsWQp2eXAwWjIxaXgzMEliaXNMdlc3cWk5ZE1LZzdHamhBdlBFWEhiSENHWnkwQ2dZRUEzaUFwOVUxbWFmcTZIcmZqCjNieGJNMCttTHBYaDBUYXk3bXhoK0tpeGtwbGNzeW5qQmFHZnhpUVpaSkZGMG5qaG51Q2ltaCs5SE4velFCN1oKbjVGZ2QxS2JjQjM2WGdObUJUQ01CMmFnb2ZqZXd5enJ3V2ZkQStVc0lzdk12aVBKbUZGeVJFUURUTGVZRnFLNwozeEJ6a05INVRDREtQTzh5TW8wQ1BnejZIZThDZ1lFQS9hN0tIVHN4eEwxZ0JiOEQ4UUhhTkR3SG4xVWsyRVlNClk3d1J1ZTcrNGlIdnc2Z2kvWnBXT1FhZHQrQ2JzWml2UjYydGovQ0hteHVjYjJSMVE2Qlh3ZXZuUkVwdVlsS2wKd0NJN09Hb1ZFK2ZXeWxHTVBvVG1nNGZCUjNNM09yeG9mT0RMenRhSzl3VllTL3JtNEZ3Mk9yZXhzUHpWalFEUgpuVnduSWxkZFIwOENnWUVBMVlmRDdnMW02M0JjNVZUUGw0UVBoQ2NJVVBaQ3E5VlNjMEw3ZDRmcmxFc2J3eHY4CklwaTV1RWRScGN6RmUwdzdVSGtQdHV2VjUyRWVQVUNxNGV0bCthOE92OXdCcDhqS2xTaVRKRFl6S3lITU80SCsKYk9GRXBRNzB1OHFBMnpRYUF2UWd6YUU0THRLN1FOOVZqVjBLUzJpZXArRkpxUVFrbEZYYmx2endvRDhDZ1lCcApiZ3czeTlNcVJkNHpaU2laTUVEa2RwSmdhTDF3V09SclNzMC9MaEdtSDU2Sy9VVFZpeUFNZ1RCcExDTG8wMkQ5CmREUHUzM01zUm5Sa1l5Yk5IVVY3cGJRdTBKUkJyc0dPTVd2VlRWbEhOWkl4OFdSTTAyVU9Bd3lUeWxHSXlxYk8KUjRyTWdxT3NkLzh6VEtwSlVtbURTN2JBck1OLzMzZytZdjhzcVl4dHh3S0JnRVYzdWprbjJQSmtPMEVlWTBZNQpYSFR2UFVzS3BNMlZRend1aXBmemFrT2pCYkg4bWlmUkJFOFR4TDZCVFlSc3crbGFYVXcxbEQ4aWJDNmRuZFRvCldWRWM3Z2kzRlh1RjBUV3p2K2dIbnFCZWFwbWdxOGE4TEhqVHRFMDNnVmo0aG1vZHJIbkVEZ3J5ajlsc0sxck0KYnp6RTlkcWdrNU5CL0c4QmwvTlJnd01BCi0tLS0tRU5EIFBSSVZBVEUgS0VZLS0tLS0K")
                .build();
    }

    public String getTlsThumbprint()    {
        return "{external=COWn2zWLZMhoewfrmSTfUeKlQPifBKekyXzjm2iGTuc=, tls=vjPd/D/f0/X3yqitf65yoUZbyeWnQU4cPDJGbr7GA7I=}";
    }

    public Secret getExternalSecret() {
        return new SecretBuilder()
                .withNewMetadata()
                    .withName("my-external-secret")
                .endMetadata()
                .addToData("tls.crt", "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUQxVENDQXIyZ0F3SUJBZ0lVUWUrQUdacXdDK0Z0ZFBiMjUyQU1ERjlaOFk0d0RRWUpLb1pJaHZjTkFRRUwKQlFBd1ZERUxNQWtHQTFVRUJoTUNRMW94RHpBTkJnTlZCQWNUQmxCeVlXZDFaVEViTUJrR0ExVUVDaE1TU21GcgpkV0lnVTJOb2IyeDZMQ0JKYm1NdU1SY3dGUVlEVlFRREV3NUpiblJsY20xbFpHbGhkR1ZEUVRBZUZ3MHhPVEV5Ck16RXhPVEl4TURCYUZ3MHlNREV4TWprd016SXhNREJhTUU0eEN6QUpCZ05WQkFZVEFrTmFNUTh3RFFZRFZRUUgKRXdaUWNtRm5kV1V4R3pBWkJnTlZCQW9URWtwaGEzVmlJRk5qYUc5c2Vpd2dTVzVqTGpFUk1BOEdBMVVFQXhNSQpSWGgwWlhKdVlXd3dnZ0VpTUEwR0NTcUdTSWIzRFFFQkFRVUFBNElCRHdBd2dnRUtBb0lCQVFDVkZDK2d1b1c2CjdmWEQ2ZC81Y2FyOHMzcktMeFRjSlgzT0Z4Ykl3K3NUNnZLOHg1cVBSVjlDS2h5ZHJzWGVhNnRQWDdhRUJETVQKL1lGd08xQWdBS0szTUwwQXFTZFZ6RktBQnIydnh3U1M5RHFKSW9zb1ovS2ZkdGZ0dHB1SnRCcWZ3eWd0QjYxWQpxU24xVnduTUFTbDdCUHluc2ZXTW40RkpqQlg4eDBKQ1lIbGhDOXVsczk5bFRSZVlRNjNHUjJNU0pqbFVpYmh1Ck83RjdZa2NmbTZKMkRrK0RzVXdiT3NoOCtHUFBGK2ZqbkU5aDJkRDVKUVdxUjc0Y2dqNnVMdE1rZ1lqWU11L2UKeTZYTkJZUkF3c1hOeU9sL1VnRW1XOVBmb3lYRTNRVnRSYVFQamg5N3RYNjlNYURNSXZML2ZFeU9NclhGWUNTYwplN0szMEpFbW9uci9BZ01CQUFHamdhUXdnYUV3RGdZRFZSMFBBUUgvQkFRREFnV2dNQjBHQTFVZEpRUVdNQlFHCkNDc0dBUVVGQndNQkJnZ3JCZ0VGQlFjREFqQU1CZ05WSFJNQkFmOEVBakFBTUIwR0ExVWREZ1FXQkJUdnJlc1cKL3l5eTlxMFRrb1lYME9sMUVlSzhiVEFmQmdOVkhTTUVHREFXZ0JUcEJ2VWdOanAvWEx4bmNmSTlnQUtCV2NSWQpPVEFpQmdOVkhSRUVHekFaZ2hjcUxqRTVNaTR4TmpndU5qUXVNVFF6TG01cGNDNXBiekFOQmdrcWhraUc5dzBCCkFRc0ZBQU9DQVFFQUgvNG1TUmtSWEZORXJwTVFkS0tPeHFjVFNrd0dNRzM1UlR4ajNjeXR2OEtNYW9VWUQvTUsKcTR5MjJLOS90OU1ybjQ2L3BNVi9aY29lZkFJQ3VRSEdnSDVHN3gxaEN4T3RKK1dCMy9oM25ZOXhnbUJwcTU5MApJZlo1NDczVnQ5RldrR3NGNU5FZnNPWkVMNE9BL3BqaStUKzFCNENWOGs1NGQ3blJkSWpMZkNSbGlVTm13WEZCCkJqeTBIOEpGZ216TFpROTNKRzRhRi9hM1RwMDhvY0xxbjZzTHkzN0pFbkJQSVBnL1ZqS3hJeGNvbUVzbFdVL28KdHZoRVNLc3V3TFcxUnkycmNJNHoyeXl5ZnIyMlFpRzdBRk5RUFdHUGlhM3FuRkYxbmxxWXI4V3VjR3Vnanp5NAphM0h0RmFnMkxwbWoxZFB6cUI4anJGZHhKY0hBVHd3UTV3PT0KLS0tLS1FTkQgQ0VSVElGSUNBVEUtLS0tLQotLS0tLUJFR0lOIENFUlRJRklDQVRFLS0tLS0KTUlJRGtUQ0NBbm1nQXdJQkFnSVVNU0Z6WEdCYnNtdVcxY3VaYU9CeUsyK01LK1V3RFFZSktvWklodmNOQVFFTApCUUF3VERFTE1Ba0dBMVVFQmhNQ1Exb3hEekFOQmdOVkJBY1RCbEJ5WVdkMVpURWJNQmtHQTFVRUNoTVNTbUZyCmRXSWdVMk5vYjJ4NkxDQkpibU11TVE4d0RRWURWUVFERXdaU2IyOTBRMEV3SGhjTk1Ua3hNak14TVRreU1UQXcKV2hjTk1qQXhNVEk1TURNeU1UQXdXakJVTVFzd0NRWURWUVFHRXdKRFdqRVBNQTBHQTFVRUJ4TUdVSEpoWjNWbApNUnN3R1FZRFZRUUtFeEpLWVd0MVlpQlRZMmh2Ykhvc0lFbHVZeTR4RnpBVkJnTlZCQU1URGtsdWRHVnliV1ZrCmFXRjBaVU5CTUlJQklqQU5CZ2txaGtpRzl3MEJBUUVGQUFPQ0FROEFNSUlCQ2dLQ0FRRUF2dW15TnFBaXp2VEIKQ0xRa3FiVDBEajI0R1ZwcVJsb0RSTGdRc0xzMFhHMCtPZVMrc0UyU2ZLTkhJeC9BK1pzUEFoTm04L1BlL01UKwpHNFBzYzgydHNpNitTZWJlYjBENExuOVI3UXVlWFpKTXlxaXhSWFlzcEZBMHA5bXhwc1NpZ0NnTFl3Y3NCVElkCm12U055VllzV2hUUHpuMXM4VUJ2SlVoenBCKzBLM1d6WkhYMEVJYVh3ZmtsM1Fob3JQZDdyQ0RUVXAzQlNwdWUKSXRENG1VcCtNV3NvSDZzRzUrazBIeUNISzVEUS9qN2xSb1Y2dGhsSkdJdkxXbmhodFRLNjVOQThsQk92Wkd6UQpQMVBaMUwreEZRUXZyZDJKMUltczZicmM4NytqM0JEZ0VxZ1YvSjJGYmtEL3JQSHVFTDRSVndqS3l2YU1Tc2crCkthU3FjQ3VJR1FJREFRQUJvMk13WVRBT0JnTlZIUThCQWY4RUJBTUNBZ1F3RHdZRFZSMFRBUUgvQkFVd0F3RUIKL3pBZEJnTlZIUTRFRmdRVTZRYjFJRFk2ZjF5OFozSHlQWUFDZ1ZuRVdEa3dId1lEVlIwakJCZ3dGb0FVSzZhZApWaHk5bmtBR1JGbXorU3MyQkNvVUhua3dEUVlKS29aSWh2Y05BUUVMQlFBRGdnRUJBTHRKRjJTY3NkVUNzT2VKCmU0N2grbG5UNHRJU2s3WVVNQk02Rlc1bFhPU05PRXhrVEs5THBOd1hpeGFQVWlLZFo1RWhURE1KUDZuNkJZaVMKV01wRU9aNmVQY3p5bVZ5cHN3KzhZUXJ6U3ByMG1UL1l3L2pTQzRwTXNXL1dBNWYwWWpGMTVidGR2U01kekd5UAp5MjlEL1B5Vy9jQnRiNlhyZGtsKzRmZUY2a1Z6bWZwWDhsSklVRmhqK0ppZmNrRWdJTkhYTHZ1SjFXWWFUbkxpClZTWi9FVUQxK0pabzZaOElFMmRsd21OQXhQc0pCSnNiUFF0eUQ4SEg1clJtWW5LaXN5Q1dvU0xIUjJRZlA4SzYKOGFNMVpxTEkvWWxmditPMzlQQnZ4eEFTZldta2VzbHp1anBUYnZTV1hRNHk1dFEvRWhvSlFjQnVsOUhWc2xiRgpKSkhTRWtnPQotLS0tLUVORCBDRVJUSUZJQ0FURS0tLS0tCi0tLS0tQkVHSU4gQ0VSVElGSUNBVEUtLS0tLQpNSUlEYURDQ0FsQ2dBd0lCQWdJVWM1Sm1sYlEwQjJDcWcxWDV6MGs1emdyU0lVc3dEUVlKS29aSWh2Y05BUUVMCkJRQXdUREVMTUFrR0ExVUVCaE1DUTFveER6QU5CZ05WQkFjVEJsQnlZV2QxWlRFYk1Ca0dBMVVFQ2hNU1NtRnIKZFdJZ1UyTm9iMng2TENCSmJtTXVNUTh3RFFZRFZRUURFd1pTYjI5MFEwRXdIaGNOTVRreE1qTXhNVGt5TVRBdwpXaGNOTWpReE1qSTVNVGt5TVRBd1dqQk1NUXN3Q1FZRFZRUUdFd0pEV2pFUE1BMEdBMVVFQnhNR1VISmhaM1ZsCk1Sc3dHUVlEVlFRS0V4SktZV3QxWWlCVFkyaHZiSG9zSUVsdVl5NHhEekFOQmdOVkJBTVRCbEp2YjNSRFFUQ0MKQVNJd0RRWUpLb1pJaHZjTkFRRUJCUUFEZ2dFUEFEQ0NBUW9DZ2dFQkFNVDdlMDUvdmoyVm5IMFl0QXRMeGlQSgpaYkoyTzZRb25ldFRiNnltT0xaU0p2d0Uyd1RUQnlXNmxXWHZaVWsvNlNwRDQ5ODZ4eXM0RUs0bkc3WWUwOGx6Cjl4OVlZSUFhU0ptcEpmcjF2SkZBNnhCQWVZTDFqNEQ0T1kyUk80Qnp2Tmtobml3SmRVQXpzZCtVQzJzTW41SE4KZ2hTQTlzejNlTjVrcXAzNzNkdFBETWgyUVRZZnMvTFgySVhuSEEzeWhRRDZlZktxTEpZR2ZYTFZTdWNhNmYrawpUTkVBVmpDQ0E1bEl5OFJ1L25LTlZVQXlvTE5CSzI2R0prRTBuNU1qMzArRVhpdFE3YlN2SEUzdm1zRFFPTnl5CkF1K0dEbEl6WWtYOXpNUjRnYnNKNDQxK3dUWE5yVWtKRmVtb1B4c3dhcEFLc3FSTlljK3dXVUJPR21ZV2xHOEMKQXdFQUFhTkNNRUF3RGdZRFZSMFBBUUgvQkFRREFnRUdNQThHQTFVZEV3RUIvd1FGTUFNQkFmOHdIUVlEVlIwTwpCQllFRkN1bW5WWWN2WjVBQmtSWnMva3JOZ1FxRkI1NU1BMEdDU3FHU0liM0RRRUJDd1VBQTRJQkFRQXhqbEMrCm5lYnNndzlZSUs3NndzTWpFZ01BNlIySzVub25nVllKZWJHZXpnejBNaW5md0IxbjVnd2xGSkRZZHJvSUhmSmQKV3pxakpCdnhqUTlxUURoYzcyaGJkK3NlelNnUFZNR29Hb2l1cmJLY3VDa3lBalZMK1M4eFNHSkY5Ti81bEtwUQpqTklMZnBtSzlxMWZlam4zYzJmcFk0eE1aRnRUWk9qZVN6SGhLdTZ2VnVLZGZYYWRuQllWOTJPWXhUeXFJVk9CCmNPMm9EUDlvQmZjWlY4N0ZTSG9zY0dUOXRnd1F5R09zbEk3YmlObTFnRmZRL1VzcEhKRVltcy8za2NKRC9vOFkKS0lKeUFPUDNwYngzc0FhTWYrVHVaUkN6WGV5SFVUUzM1a3VoYjdvdEFTYmh1amlEaHRTeHFwL05CT3lBL2tmeApuQXN6SUdEMVdXYnBOSjMrCi0tLS0tRU5EIENFUlRJRklDQVRFLS0tLS0K")
                .addToData("tls.key", "LS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tCk1JSUV2Z0lCQURBTkJna3Foa2lHOXcwQkFRRUZBQVNDQktnd2dnU2tBZ0VBQW9JQkFRQ1ZGQytndW9XNjdmWEQKNmQvNWNhcjhzM3JLTHhUY0pYM09GeGJJdytzVDZ2Szh4NXFQUlY5Q0toeWRyc1hlYTZ0UFg3YUVCRE1UL1lGdwpPMUFnQUtLM01MMEFxU2RWekZLQUJyMnZ4d1NTOURxSklvc29aL0tmZHRmdHRwdUp0QnFmd3lndEI2MVlxU24xClZ3bk1BU2w3QlB5bnNmV01uNEZKakJYOHgwSkNZSGxoQzl1bHM5OWxUUmVZUTYzR1IyTVNKamxVaWJodU83RjcKWWtjZm02SjJEaytEc1V3Yk9zaDgrR1BQRitmam5FOWgyZEQ1SlFXcVI3NGNnajZ1THRNa2dZallNdS9leTZYTgpCWVJBd3NYTnlPbC9VZ0VtVzlQZm95WEUzUVZ0UmFRUGpoOTd0WDY5TWFETUl2TC9mRXlPTXJYRllDU2NlN0szCjBKRW1vbnIvQWdNQkFBRUNnZ0VBTzVhNkF2RUxpMUNhc0JqSDRobEJVNGthUjc3U0E3MG9zRHdpYTFXRW5ZMkkKUVZVM3ZwVG9JclphZ2R6ZVVxMk82RWRGMlRja2c1VU5MQ05KUDhHQlNPQStiQWt4SStac0E2aXVJWmpYaHpZQQpQOWlDN3orOWgyZ2xuMnNpZU1SNDcrcytIK0cxdEg3SnVydHp1d3VyM1BSOVdUcVZBQVN4MVFnZHNkQ2o5NHVrClJzdGsrSGhjM2thT1U0UHM1cWFWbWJZdFB6ZlVibjhoK0xZOWNpZGxQanhiaWZCMTdiK0FaSW9mS2FTb1hEVG4KRks4Wmk1V056cklFdm1TQVZJT00vMHhPbUpnTXdIWUdvUW9PbWJ1TGl0VEFOemQwTmFUVFcyTDV0dlZYa1psOApGZlRrOFRGZ2p2RUpSdExEdklHd3VlS1I1ZE51cGpWSUQ1ek55c0NLSVFLQmdRREFRWVhQclVtd0xheXBhQU5aCmNuY21sL1NNbmowRklwcTJId3Ayb05JRTU1ZlBzZ04yK2tpQklPNXVxYkFGa2U5aGJldTVqS3FLT1hRVFFya3UKa04ybmZvRGJNYklVS29vZXowZyt3REdHeWhJOThzS1REdTZmZTd2Z1FSR3d5ZzIvMUhlRlZVbFMzSHRGbm9aWQpMbnBubXUwcmxoSXg2R2MzaE9PZ1VQN2Jvd0tCZ1FER2dkYUpCVGtuRDVDcjJpYzZ4c2d1WnV6RTd5bzUrZ05uCjREcjc2NXNRc3hNVGhIbjJJMXNpMldBeFl0RmVROGxHMCs3ai9vYjZOUHRaWWg1eXBud2RsbHFiRXVtNUxzYVoKTFZnY09hMGsxeEhxVSt3VUxJWm9hcmlxWXhYUHMvcEU4ZHoralQ3Wld4WmI0aGJ1WmRrU2lJWWVWTzlpaGhKagpKbDZGdTRFWTlRS0JnUUN2S2tPL3J4UUhaK1g3eDEvZDNGUEJId3ZhSHNaYjZtWnBicWk2NHRYWFVDYmFQa2UzCjNGdTVBd2NhWHBLWTBKajQvUXliMXhUK3NWQVh5R0F1bENEUDNZdUxxcUNralFtZy9wekZSNWtZUlA0UDRTSDAKbU5ORERacGt2UVJnUGdmKzhwY2ZMVkNNSllSUEx4c2FOdWFoaE45NEtkaFVEbm9VZEloc1piOSszd0tCZ0d3cApBTGttQkc4WkZ3M2NUdlhDcS81RWpJdjllTGVnVjB5NUs4cHFKTktqa0NoWlRZN2swdHFaTU1XWC8xWnFmdmc5CnIvUEFrdEV3SHlnancwMWJFMU9Yd2dTdStIU3pYUGpIY1RQbjVVU21meGQ3NUsxVldXTDVpMmNqbUJYVkRlK1YKRFlJUmVnWTZrR00rUEpwbkdqRHovSWY0WlhyOGJIWmp5S3I3Y0tzbEFvR0JBTG1TbS8ydkptd21VTzJSQjZEQwpjb1ZrTEZLNkROUmhQaGw4c3NOdDRBWnA4YUJtVzloZC9TditWOHhoNzl6OGhPUlF5cjZoNDVVRjlySXhJb1kyCll3MllidkkyYzRiZlZEQWpGY3U3UEZ3MzVoOFJPRytrMStoNDZQUkhKY1F2dzJxSnN2NnhKOVZLaEdzcEQxdUgKeUVHOEQxRGtVM1FVOElTN01ybUR6Mk9YCi0tLS0tRU5EIFBSSVZBVEUgS0VZLS0tLS0K")
                .build();
    }

    public Pod getPod(StatefulSet sts) {
        return new PodBuilder()
                .withNewMetadataLike(sts.getSpec().getTemplate().getMetadata())
                    .withName(KafkaCluster.kafkaClusterName(clusterName) + "-0")
                .endMetadata()
                .withNewSpecLike(sts.getSpec().getTemplate().getSpec())
                .endSpec()
                .build();
    }

    @Test
    public void testPodToRestartIsEmptyWhenCustomCertAnnotationsHaveMatchingThumbprints(VertxTestContext context) {
        Checkpoint async = context.checkpoint();
        operator.createOrUpdate(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, clusterName), kafka)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                StatefulSet reconcileSts = client.apps().statefulSets().inNamespace(namespace).withName(KafkaResources.kafkaStatefulSetName(clusterName)).get();
                assertThat(reconcileSts.getSpec().getTemplate().getMetadata().getAnnotations(),
                        hasEntry(KafkaCluster.ANNO_STRIMZI_CUSTOM_LISTENER_CERT_THUMBPRINTS, getTlsThumbprint()));

                assertThat(functionArgumentCaptor, hasSize(1));
                assertThat(functionArgumentCaptor.get(0).apply(getPod(reconcileSts)), empty());
                async.flag();
            })));
    }

    @Test
    public void testPodToRestartNonemptyWhenCustomCertTlsListenerThumbprintAnnotationsNotMatchingThumbprint(VertxTestContext context) {
        Checkpoint async = context.checkpoint();
        operator.createOrUpdate(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, clusterName), kafka)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                StatefulSet reconcileSts = client.apps().statefulSets().inNamespace(namespace).withName(KafkaResources.kafkaStatefulSetName(clusterName)).get();
                assertThat(reconcileSts.getSpec().getTemplate().getMetadata().getAnnotations(),
                        hasEntry(KafkaCluster.ANNO_STRIMZI_CUSTOM_LISTENER_CERT_THUMBPRINTS, getTlsThumbprint()));

                assertThat(functionArgumentCaptor, hasSize(1));
                Function<Pod, List<String>> isPodToRestart = functionArgumentCaptor.get(0);

                Pod pod = getPod(reconcileSts);
                assertThat("There are no changes in broker config, the restart should not be needed",
                        isPodToRestart.apply(pod), empty());

                pod.getMetadata().getAnnotations().put(KafkaCluster.ANNO_STRIMZI_CUSTOM_LISTENER_CERT_THUMBPRINTS,
                        Base64.getEncoder().encodeToString("Not the right one!".getBytes()));
                assertThat("Tls listener thumbprint annotation changed, pod should need restart",
                        isPodToRestart.apply(pod).get(0),
                        equalTo("custom certificate one or more listeners changed"));

                async.flag();
            })));
    }

    @Test
    public void testPodToRestartTrueWhenCustomCertExternalListenerThumbprintAnnotationsNotMatchingThumbprint(VertxTestContext context) {
        Checkpoint async = context.checkpoint();
        operator.createOrUpdate(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, clusterName), kafka)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                StatefulSet reconcileSts = client.apps().statefulSets().inNamespace(namespace).withName(KafkaResources.kafkaStatefulSetName(clusterName)).get();
                assertThat(reconcileSts.getSpec().getTemplate().getMetadata().getAnnotations(),
                        hasEntry(KafkaCluster.ANNO_STRIMZI_CUSTOM_LISTENER_CERT_THUMBPRINTS, getTlsThumbprint()));

                assertThat(functionArgumentCaptor, hasSize(1));
                Function<Pod, List<String>> isPodToRestart = functionArgumentCaptor.get(0);

                Pod pod = getPod(reconcileSts);

                assertThat("There are no changes in broker config, the restart should not be needed",
                        isPodToRestart.apply(pod), empty());

                pod.getMetadata().getAnnotations().put(KafkaCluster.ANNO_STRIMZI_CUSTOM_LISTENER_CERT_THUMBPRINTS,
                        Base64.getEncoder().encodeToString("Not the right one!".getBytes()));

                assertThat(isPodToRestart.apply(pod).get(0),
                        equalTo("custom certificate one or more listeners changed"));

                async.flag();
            })));
    }

    @Test
    public void testPodToRestartIsEmptyAndNoCustomCertAnnotationsWhenNoCustomCertificates(VertxTestContext context) {
        kafka = new KafkaBuilder(createKafka())
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                    .withName("tls")
                                    .withPort(9093)
                                    .withType(KafkaListenerType.INTERNAL)
                                    .withTls(true)
                                    .build(),
                                new GenericKafkaListenerBuilder()
                                    .withName("external")
                                    .withPort(9094)
                                    .withType(KafkaListenerType.NODEPORT)
                                    .withTls(true)
                                    .build())
                    .endKafka()
                .endSpec()
                .build();
        Crds.kafkaOperation(client).inNamespace(namespace).withName(clusterName).patch(kafka);


        Checkpoint async = context.checkpoint();
        operator.createOrUpdate(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, clusterName), kafka)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                assertThat(functionArgumentCaptor, hasSize(1));

                StatefulSet reconcileSts = client.apps().statefulSets().inNamespace(namespace).withName(KafkaResources.kafkaStatefulSetName(clusterName)).get();
                assertThat(reconcileSts.getSpec().getTemplate().getMetadata().getAnnotations(),
                        not(hasKey(KafkaCluster.ANNO_STRIMZI_CUSTOM_LISTENER_CERT_THUMBPRINTS)));

                List<Function<Pod, List<String>>> capturedFunctions = functionArgumentCaptor;
                assertThat(capturedFunctions, hasSize(1));
                Function<Pod, List<String>> isPodToRestart = capturedFunctions.get(0);
                assertThat(isPodToRestart.apply(getPod(reconcileSts)), empty());

                async.flag();
            })));
    }
}
