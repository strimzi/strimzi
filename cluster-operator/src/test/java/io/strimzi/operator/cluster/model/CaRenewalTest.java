/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.strimzi.certs.CertAndKey;
import io.strimzi.certs.Subject;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;
import io.vertx.junit5.VertxExtension;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@ParallelSuite
@ExtendWith(VertxExtension.class)
public class CaRenewalTest {
    @ParallelTest
    public void renewalOfStatefulSetCertificatesWithNullSecret() throws IOException {
        Ca mockedCa = new Ca(Reconciliation.DUMMY_RECONCILIATION, null, null, null, null, null, null, null, 2, 1, true, null) {
            private AtomicInteger invocationCount = new AtomicInteger(0);

            @Override
            public boolean certRenewed() {
                return false;
            }

            @Override
            public boolean isExpiring(Secret secret, String certKey)  {
                return false;
            }

            @Override
            protected CertAndKey generateSignedCert(Subject subject,
                                                    File csrFile, File keyFile, File certFile, File keyStoreFile) throws IOException {
                int index = invocationCount.getAndIncrement();

                return new CertAndKey(
                        ("new-key" + index).getBytes(),
                        ("new-cert" + index).getBytes(),
                        ("new-truststore" + index).getBytes(),
                        ("new-keystore" + index).getBytes(),
                        "new-password" + index
                );
            }
        };

        int replicas = 3;
        Function<Integer, Subject> subjectFn = i -> new Subject.Builder().build();
        Function<Integer, String> podNameFn = i -> "pod" + i;
        boolean isMaintenanceTimeWindowsSatisfied = true;

        Map<String, CertAndKey> newCerts = mockedCa.maybeCopyOrGenerateCerts(Reconciliation.DUMMY_RECONCILIATION, replicas,
                subjectFn,
                null,
                podNameFn,
                isMaintenanceTimeWindowsSatisfied);

        assertThat(new String(newCerts.get("pod0").cert()), is("new-cert0"));
        assertThat(new String(newCerts.get("pod0").key()), is("new-key0"));
        assertThat(new String(newCerts.get("pod0").keyStore()), is("new-keystore0"));
        assertThat(newCerts.get("pod0").storePassword(), is("new-password0"));

        assertThat(new String(newCerts.get("pod1").cert()), is("new-cert1"));
        assertThat(new String(newCerts.get("pod1").key()), is("new-key1"));
        assertThat(new String(newCerts.get("pod1").keyStore()), is("new-keystore1"));
        assertThat(newCerts.get("pod1").storePassword(), is("new-password1"));

        assertThat(new String(newCerts.get("pod2").cert()), is("new-cert2"));
        assertThat(new String(newCerts.get("pod2").key()), is("new-key2"));
        assertThat(new String(newCerts.get("pod2").keyStore()), is("new-keystore2"));
        assertThat(newCerts.get("pod2").storePassword(), is("new-password2"));
    }

    @ParallelTest
    public void renewalOfStatefulSetCertificatesWithCaRenewal() throws IOException {
        Ca mockedCa = new Ca(Reconciliation.DUMMY_RECONCILIATION, null, null, null, null, null, null, null, 2, 1, true, null) {
            private AtomicInteger invocationCount = new AtomicInteger(0);

            @Override
            public boolean certRenewed() {
                return true;
            }

            @Override
            public boolean isExpiring(Secret secret, String certKey)  {
                return false;
            }

            @Override
            protected CertAndKey generateSignedCert(Subject subject,
                                                    File csrFile, File keyFile, File certFile, File keyStoreFile) throws IOException {
                int index = invocationCount.getAndIncrement();

                return new CertAndKey(
                        ("new-key" + index).getBytes(),
                        ("new-cert" + index).getBytes(),
                        ("new-truststore" + index).getBytes(),
                        ("new-keystore" + index).getBytes(),
                        "new-password" + index
                );
            }
        };

        Secret initialSecret = new SecretBuilder()
                .withNewMetadata()
                    .withName("test-secret")
                .endMetadata()
                .addToData("pod0.crt", Base64.getEncoder().encodeToString("old-cert".getBytes()))
                .addToData("pod0.key", Base64.getEncoder().encodeToString("old-key".getBytes()))
                .addToData("pod0.p12", Base64.getEncoder().encodeToString("old-keystore".getBytes()))
                .addToData("pod0.password", Base64.getEncoder().encodeToString("old-password".getBytes()))
                .addToData("pod1.crt", Base64.getEncoder().encodeToString("old-cert".getBytes()))
                .addToData("pod1.key", Base64.getEncoder().encodeToString("old-key".getBytes()))
                .addToData("pod1.p12", Base64.getEncoder().encodeToString("old-keystore".getBytes()))
                .addToData("pod1.password", Base64.getEncoder().encodeToString("old-password".getBytes()))
                .addToData("pod2.crt", Base64.getEncoder().encodeToString("old-cert".getBytes()))
                .addToData("pod2.key", Base64.getEncoder().encodeToString("old-key".getBytes()))
                .addToData("pod2.p12", Base64.getEncoder().encodeToString("old-keystore".getBytes()))
                .addToData("pod2.password", Base64.getEncoder().encodeToString("old-password".getBytes()))
                .build();

        int replicas = 3;
        Function<Integer, Subject> subjectFn = i -> new Subject.Builder().build();
        Function<Integer, String> podNameFn = i -> "pod" + i;
        boolean isMaintenanceTimeWindowsSatisfied = true;

        Map<String, CertAndKey> newCerts = mockedCa.maybeCopyOrGenerateCerts(Reconciliation.DUMMY_RECONCILIATION,
                replicas,
                subjectFn,
                initialSecret,
                podNameFn,
                isMaintenanceTimeWindowsSatisfied);

        assertThat(new String(newCerts.get("pod0").cert()), is("new-cert0"));
        assertThat(new String(newCerts.get("pod0").key()), is("new-key0"));
        assertThat(new String(newCerts.get("pod0").keyStore()), is("new-keystore0"));
        assertThat(newCerts.get("pod0").storePassword(), is("new-password0"));

        assertThat(new String(newCerts.get("pod1").cert()), is("new-cert1"));
        assertThat(new String(newCerts.get("pod1").key()), is("new-key1"));
        assertThat(new String(newCerts.get("pod1").keyStore()), is("new-keystore1"));
        assertThat(newCerts.get("pod1").storePassword(), is("new-password1"));

        assertThat(new String(newCerts.get("pod2").cert()), is("new-cert2"));
        assertThat(new String(newCerts.get("pod2").key()), is("new-key2"));
        assertThat(new String(newCerts.get("pod2").keyStore()), is("new-keystore2"));
        assertThat(newCerts.get("pod2").storePassword(), is("new-password2"));
    }

    @ParallelTest
    public void renewalOfStatefulSetCertificatesDelayedRenewalInWindow() throws IOException {
        Ca mockedCa = new Ca(Reconciliation.DUMMY_RECONCILIATION, null, null, null, null, null, null, null, 2, 1, true, null) {
            private AtomicInteger invocationCount = new AtomicInteger(0);

            @Override
            public boolean certRenewed() {
                return false;
            }

            @Override
            public boolean isExpiring(Secret secret, String certKey)  {
                return true;
            }

            @Override
            protected boolean certSubjectChanged(CertAndKey certAndKey, Subject desiredSubject, String podName)    {
                return false;
            }

            @Override
            public X509Certificate getAsX509Certificate(Secret secret, String key)    {
                return null;
            }

            @Override
            protected CertAndKey generateSignedCert(Subject subject,
                                                    File csrFile, File keyFile, File certFile, File keyStoreFile) throws IOException {
                int index = invocationCount.getAndIncrement();

                return new CertAndKey(
                        ("new-key" + index).getBytes(),
                        ("new-cert" + index).getBytes(),
                        ("new-truststore" + index).getBytes(),
                        ("new-keystore" + index).getBytes(),
                        "new-password" + index
                );
            }
        };

        Secret initialSecret = new SecretBuilder()
                .withNewMetadata()
                .withName("test-secret")
                .endMetadata()
                .addToData("pod0.crt", Base64.getEncoder().encodeToString("old-cert".getBytes()))
                .addToData("pod0.key", Base64.getEncoder().encodeToString("old-key".getBytes()))
                .addToData("pod0.p12", Base64.getEncoder().encodeToString("old-keystore".getBytes()))
                .addToData("pod0.password", Base64.getEncoder().encodeToString("old-password".getBytes()))
                .addToData("pod1.crt", Base64.getEncoder().encodeToString("old-cert".getBytes()))
                .addToData("pod1.key", Base64.getEncoder().encodeToString("old-key".getBytes()))
                .addToData("pod1.p12", Base64.getEncoder().encodeToString("old-keystore".getBytes()))
                .addToData("pod1.password", Base64.getEncoder().encodeToString("old-password".getBytes()))
                .addToData("pod2.crt", Base64.getEncoder().encodeToString("old-cert".getBytes()))
                .addToData("pod2.key", Base64.getEncoder().encodeToString("old-key".getBytes()))
                .addToData("pod2.p12", Base64.getEncoder().encodeToString("old-keystore".getBytes()))
                .addToData("pod2.password", Base64.getEncoder().encodeToString("old-password".getBytes()))
                .build();

        int replicas = 3;
        Function<Integer, Subject> subjectFn = i -> new Subject.Builder().build();
        Function<Integer, String> podNameFn = i -> "pod" + i;
        boolean isMaintenanceTimeWindowsSatisfied = true;

        Map<String, CertAndKey> newCerts = mockedCa.maybeCopyOrGenerateCerts(Reconciliation.DUMMY_RECONCILIATION,
                replicas,
                subjectFn,
                initialSecret,
                podNameFn,
                isMaintenanceTimeWindowsSatisfied);

        assertThat(new String(newCerts.get("pod0").cert()), is("new-cert0"));
        assertThat(new String(newCerts.get("pod0").key()), is("new-key0"));
        assertThat(new String(newCerts.get("pod0").keyStore()), is("new-keystore0"));
        assertThat(newCerts.get("pod0").storePassword(), is("new-password0"));

        assertThat(new String(newCerts.get("pod1").cert()), is("new-cert1"));
        assertThat(new String(newCerts.get("pod1").key()), is("new-key1"));
        assertThat(new String(newCerts.get("pod1").keyStore()), is("new-keystore1"));
        assertThat(newCerts.get("pod1").storePassword(), is("new-password1"));

        assertThat(new String(newCerts.get("pod2").cert()), is("new-cert2"));
        assertThat(new String(newCerts.get("pod2").key()), is("new-key2"));
        assertThat(new String(newCerts.get("pod2").keyStore()), is("new-keystore2"));
        assertThat(newCerts.get("pod2").storePassword(), is("new-password2"));
    }

    @ParallelTest
    public void renewalOfStatefulSetCertificatesDelayedRenewalOutsideWindow() throws IOException {
        Ca mockedCa = new Ca(Reconciliation.DUMMY_RECONCILIATION, null, null, null, null, null, null, null, 2, 1, true, null) {
            private AtomicInteger invocationCount = new AtomicInteger(0);

            @Override
            public boolean certRenewed() {
                return false;
            }

            @Override
            public boolean isExpiring(Secret secret, String certKey)  {
                return true;
            }

            @Override
            protected boolean certSubjectChanged(CertAndKey certAndKey, Subject desiredSubject, String podName)    {
                return false;
            }

            @Override
            public X509Certificate getAsX509Certificate(Secret secret, String key)    {
                return null;
            }

            @Override
            protected CertAndKey generateSignedCert(Subject subject,
                                                    File csrFile, File keyFile, File certFile, File keyStoreFile) throws IOException {
                int index = invocationCount.getAndIncrement();

                return new CertAndKey(
                        ("new-key" + index).getBytes(),
                        ("new-cert" + index).getBytes(),
                        ("new-truststore" + index).getBytes(),
                        ("new-keystore" + index).getBytes(),
                        "new-password" + index
                );
            }
        };

        Secret initialSecret = new SecretBuilder()
                .withNewMetadata()
                .withName("test-secret")
                .endMetadata()
                .addToData("pod0.crt", Base64.getEncoder().encodeToString("old-cert".getBytes()))
                .addToData("pod0.key", Base64.getEncoder().encodeToString("old-key".getBytes()))
                .addToData("pod0.p12", Base64.getEncoder().encodeToString("old-keystore".getBytes()))
                .addToData("pod0.password", Base64.getEncoder().encodeToString("old-password".getBytes()))
                .addToData("pod1.crt", Base64.getEncoder().encodeToString("old-cert".getBytes()))
                .addToData("pod1.key", Base64.getEncoder().encodeToString("old-key".getBytes()))
                .addToData("pod1.p12", Base64.getEncoder().encodeToString("old-keystore".getBytes()))
                .addToData("pod1.password", Base64.getEncoder().encodeToString("old-password".getBytes()))
                .addToData("pod2.crt", Base64.getEncoder().encodeToString("old-cert".getBytes()))
                .addToData("pod2.key", Base64.getEncoder().encodeToString("old-key".getBytes()))
                .addToData("pod2.p12", Base64.getEncoder().encodeToString("old-keystore".getBytes()))
                .addToData("pod2.password", Base64.getEncoder().encodeToString("old-password".getBytes()))
                .build();

        int replicas = 3;
        Function<Integer, Subject> subjectFn = i -> new Subject.Builder().build();
        Function<Integer, String> podNameFn = i -> "pod" + i;
        boolean isMaintenanceTimeWindowsSatisfied = false;

        Map<String, CertAndKey> newCerts = mockedCa.maybeCopyOrGenerateCerts(Reconciliation.DUMMY_RECONCILIATION,
                replicas,
                subjectFn,
                initialSecret,
                podNameFn,
                isMaintenanceTimeWindowsSatisfied);

        assertThat(new String(newCerts.get("pod0").cert()), is("old-cert"));
        assertThat(new String(newCerts.get("pod0").key()), is("old-key"));
        assertThat(new String(newCerts.get("pod0").keyStore()), is("old-keystore"));
        assertThat(newCerts.get("pod0").storePassword(), is("old-password"));

        assertThat(new String(newCerts.get("pod1").cert()), is("old-cert"));
        assertThat(new String(newCerts.get("pod1").key()), is("old-key"));
        assertThat(new String(newCerts.get("pod1").keyStore()), is("old-keystore"));
        assertThat(newCerts.get("pod1").storePassword(), is("old-password"));

        assertThat(new String(newCerts.get("pod2").cert()), is("old-cert"));
        assertThat(new String(newCerts.get("pod2").key()), is("old-key"));
        assertThat(new String(newCerts.get("pod2").keyStore()), is("old-keystore"));
        assertThat(newCerts.get("pod2").storePassword(), is("old-password"));
    }
}