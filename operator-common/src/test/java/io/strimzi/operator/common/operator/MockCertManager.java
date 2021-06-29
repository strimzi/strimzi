/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator;

import io.strimzi.certs.CertManager;
import io.strimzi.certs.Subject;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.time.ZonedDateTime;
import java.util.Base64;
import java.util.List;

public class MockCertManager implements CertManager {

    private static final String CLUSTER_KEY = "-----BEGIN PRIVATE KEY-----\n" +
            "MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDbFnJj90sKoM35\n" +
            "VszJsfwNvO5dshoeFIb2idf7h+l0h3GMv29j+1XtmLJGzxiYy320KFZr3IKWbq+D\n" +
            "abqdlqEqZm9NZ1Kq9d7mB10zulQce5JwVZ3FqpCmLku2jHCaDXzTKC3T/Xp0O9Oe\n" +
            "8+42ysSMCTd8p8aZ4vAyJMCKcoyVCGHrUWVba40D7cQNOlhJplSzHZdLFYZ13kwz\n" +
            "pT5GpDEPhGVmtF8qV918lSxvdpuepyeFdOSYY88FEMMLLrlZG4QCPyES4FpcUXMz\n" +
            "zvZeLIlZnKNIYbao3Kx+yZv//wjC80/pqdyoZ5+K5hDxjby2+f+2dh0TadKRZC2p\n" +
            "p+j3/z63AgMBAAECggEBAJN5iai25uGRmvSzNAi08VkCC2YwpBoJcUv1P9jGBST2\n" +
            "oz2+AypHHfFgruixMPpxR/2EhZ/3gEPo3+ZSvlaj9XrIFzYATgpclR08acWPMF03\n" +
            "5TwOtbQ/+zyRv09zO7zHRXYR/r9LSimBuBKwWnKxjRpCfgJAIZSmyU7HpH/NWcpa\n" +
            "6khQen5zQVsjnlv3L5OSN8exP8okVYY01JmrKTyo3IWsOiTogr1ebe84xvG2QzkV\n" +
            "fdL99poVfXXSBivLwgGPiCNkZFtwcjDRECzBHAcrwYT1AIHsvsWvdbrXilAh/388\n" +
            "1v3HWw7lfnJi8WV9iAplq1YOEwqKN6z5Y/wwAZQafHECgYEA8F3lGtOUIMeaTXeI\n" +
            "9xlXAM1MXQPPxguMFS9ilcnqlTdNJjJ18FBWM/Kn73RF9mTCQCYn+5o6MmoyMQIq\n" +
            "rreBchP7Bzg7RCKVvPnippqwVDeLY7khdA0I1lH6mD7urhBvO5Fby3wKZeJfi2N9\n" +
            "suQzEuRDEw5EDHQf/0rP4RBnrg8CgYEA6VZBUKuur885Q/knRUKdIG/mXYAcvmAV\n" +
            "I6kjFCVRx7lQ8xLTiagKo0SKr0TM0Qf1+4vqTUZCOBtGlW/UBdGW8yPvXegyMHQg\n" +
            "pMoNCTnxgXS77f4pnluQRQux8M8oWJnf11oGxDHhLw0T7kqBHl9K/dw/cItRS5Ui\n" +
            "Dch/2YDMDNkCgYBNqZjXxRrsSHHTq9amOBrDWJHez9d3Hs4BHlFVImtYEQktWUp/\n" +
            "/gUMPdAC72eXh9C3l1x9z8QT+/oBmbiewQ3jBQ+rsoB7sEz/RSH1QK/OVjAEZZGo\n" +
            "hHmhfdVhEZxew1KdRYcKRSa66pyCVgAMJ+1UokoFwys7dt3Lx6lJB9roAwKBgCRN\n" +
            "OxQl4aOQhcRBew6XcoKdZiWdzNsBb8iAg+iadcKw3hszDp4X+q+z9i+WcJcEugxM\n" +
            "lEM5bwvzkmOlZkMRfH6PVKozebt4FawNk0GgNiaB1ssMA8WTUTqsux5P3GMMbXq/\n" +
            "ktXrPLFpQ3SLOtNS2APuxB/qTNeJeCbUzq80DorhAoGBALAU7AWjjxgjCJlVKcfc\n" +
            "iFeFh0W3HnNMQfn/ukha2Mg4Nl4GdLN18wx5VGpSCMEZ415cCy5S0SXxuUE5l6G1\n" +
            "Pb2gYF9JyGhXJLMF2x9k4he4j/qHu9RCvixYlbb/jN59aifJxTeOG53g0Fo+H9U1\n" +
            "432A3bs1MPT1Ew7zgnxjznt8\n" +
            "-----END PRIVATE KEY-----\n";
    private static final String CLUSTER_CERT = "-----BEGIN CERTIFICATE-----\n" +
            "MIIDhjCCAm6gAwIBAgIJANzx2pPcYgmlMA0GCSqGSIb3DQEBCwUAMFcxCzAJBgNV\n" +
            "BAYTAlhYMRUwEwYDVQQHDAxEZWZhdWx0IENpdHkxHDAaBgNVBAoME0RlZmF1bHQg\n" +
            "Q29tcGFueSBMdGQxEzARBgNVBAMMCmNsdXN0ZXItY2EwIBcNMTgwODIzMTYxOTU0\n" +
            "WhgPMjExODA3MzAxNjE5NTRaMFcxCzAJBgNVBAYTAlhYMRUwEwYDVQQHDAxEZWZh\n" +
            "dWx0IENpdHkxHDAaBgNVBAoME0RlZmF1bHQgQ29tcGFueSBMdGQxEzARBgNVBAMM\n" +
            "CmNsdXN0ZXItY2EwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDbFnJj\n" +
            "90sKoM35VszJsfwNvO5dshoeFIb2idf7h+l0h3GMv29j+1XtmLJGzxiYy320KFZr\n" +
            "3IKWbq+DabqdlqEqZm9NZ1Kq9d7mB10zulQce5JwVZ3FqpCmLku2jHCaDXzTKC3T\n" +
            "/Xp0O9Oe8+42ysSMCTd8p8aZ4vAyJMCKcoyVCGHrUWVba40D7cQNOlhJplSzHZdL\n" +
            "FYZ13kwzpT5GpDEPhGVmtF8qV918lSxvdpuepyeFdOSYY88FEMMLLrlZG4QCPyES\n" +
            "4FpcUXMzzvZeLIlZnKNIYbao3Kx+yZv//wjC80/pqdyoZ5+K5hDxjby2+f+2dh0T\n" +
            "adKRZC2pp+j3/z63AgMBAAGjUzBRMB0GA1UdDgQWBBThuvddCb/5TPSKYNOHkCTL\n" +
            "VghhRzAfBgNVHSMEGDAWgBThuvddCb/5TPSKYNOHkCTLVghhRzAPBgNVHRMBAf8E\n" +
            "BTADAQH/MA0GCSqGSIb3DQEBCwUAA4IBAQBA6oTI27dJgbVtyWxQWznKrkznZ9+t\n" +
            "mQQGbpfl9zEg7/0X7fFb+m84QHro+aNnQ4kTgZ6QBvusIpwfx1F6lQrraVrPr142\n" +
            "4DqGmY9xReNu/fj+C+8lTI5PA+mE7tMrLpQvKxI+AMttvlz8eo1SITUA+kJEiWZX\n" +
            "mjvyHXmhic4K8SnnB0gnFzHN4y09wLqRMNCRH+aI+sa9Wu8cqvpTqlelVcYV83zu\n" +
            "ydx4VZkC+zTzjI418znN/NU2CMpxLZNl0/zCrspID7v34NRmJ1AHFcrn7/XhsSvz\n" +
            "D0z+vgrfionoRhyWUDh7POlWwdUOWiBDBOFrkgeKNphSC0glYFN+2IW7\n" +
            "-----END CERTIFICATE-----\n";
    private static final String CLIENTS_KEY = "-----BEGIN PRIVATE KEY-----\n" +
            "MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCWws5FEJOpfZ/s\n" +
            "FJWYbYdJVxmZB+5PjCwA2TUZxF/3P4w/5g2KZaXNy89AfBC5vRDRgyDyj/RwcDg8\n" +
            "0kDGKobcGhTx5YkWoNvR/2WuTN6KC8DM78bfEREDHDxiXfAXrMIi7Ux2FvUX13l7\n" +
            "6Sp9kiG3ETLjFom3n/qhg1ITJqPJSJi3tey0o2Pd5Arv0MIhQyep++URtZfND5fg\n" +
            "F5x7hgnSf9Q1P1dJnVadu+ohUmmG7g+zX4rTqjN2jmHcf9V4lLKdPGWwLQEGnP9y\n" +
            "Dqlm8x12M/BcIJasRgcciVsKYFuXe09NEYBvUjW8L6gaQ6U9wcYZ2MlKW/8LMGkS\n" +
            "FfO4quAJAgMBAAECggEAQ1NdsEQV3UQHrfMHV1naZ6so+EktaILNh9d4OjiTLqRH\n" +
            "aqW++EYqhDv3IvIEuh2vrBCmHwygebHzu12dpaGKNjLDlb8OuHc/k4k9jFgxrW5Q\n" +
            "PHT719QUR9JNORSASuJQlC5qzfW0oGAOlYJsAkXHHqzkj7sZ51HfKE+v0HOaAyHj\n" +
            "8gOeBNk1Mtb3Sj5mXpWFQGpXXuG01Vsjj7Nj/91a4KtWAWOqeagc2Bk+C0aZ7d1p\n" +
            "SQcLVWjJYwoejgCc2elZxzbfmDtVSAgFtdTPxwf9uflMducTfp/RyaQbzuYSrSmz\n" +
            "rnZq/59i9lYl314rjjkCusDaDSPdK5QziN54tQ+BcQKBgQDE9ulPecHtZhOsI9zT\n" +
            "J+xTJtZq1w8kFV5jMqXnL3jAFBXsC3s02KLq36ppvf8kVzUHrHE+DiWnHKEIiy/U\n" +
            "luMnPvJb/6qqdQNDpcrF+CE2JevvoPl5hrKdyzAI4TNu96aU+9qVrO2rB7bWBvlA\n" +
            "dVwIZ8zkk3pwbdEj9rYpMA1VVQKBgQDD8rJAEd9fLtX53NQh8XWEJ1dEfncmg/ib\n" +
            "0vyoYlqSDjPTot85sCunVZNHwUoKUsukzi+Tc9hxaXCjEB6ICVeXqWc4PYnbK79H\n" +
            "N+2X6YaO/rKAzbxM1F/Km3IzzvoXFJnPG4hxvBmpdApKgBGOVixnjD7PzNz4jh9u\n" +
            "1qhDocdf5QKBgQCDsLqporTgr0Ez9P5uR+Egb3UpFgVPkOH83R5Dhl/rvQIzQjHs\n" +
            "UXQMKeNcs+XlPFF+gfNtFDRkmSWp+rXOI9xYnyOYE0belUHLdwwudQpvk8c9/pkO\n" +
            "gdrm2bWSGlAzP22nawTo0ihOE+hRDXSVfmI8VHqP0XMpvKL6srd0rmYbyQKBgAYD\n" +
            "PXr/0WXfTwuSviOogB2lA2WDp+5ToF5PtBcKpZLTwr1cwxLHGB/TXWiXQslcTwlo\n" +
            "lkclB+A7BwzJ4tXzy29I8HTmVoOWLRFnYvAFZ26d3CZdqciFv8a8zF1QnZX1uN6F\n" +
            "DsPGrNbpS6OLmH5QoJ4wzICd3a321noVNiaVIUQNAoGAYu4RrGcBKRuy75lfKARD\n" +
            "gNxxVlvuI33ieK/3A9nUWc3LXl5D/yiSePCUs4giOwi2gFrGjcmIqLXZE5XUYGEu\n" +
            "zXWWQCGbMqyX15/A2/eTuj658F292nkSyU/5U2999WjCm79sfnGJB1zavfv2fzGK\n" +
            "g4trXCUkjAVG3Toaq05saGM=\n" +
            "-----END PRIVATE KEY-----\n";
    private static final String CLIENTS_CERT = "-----BEGIN CERTIFICATE-----\n" +
            "MIIDhjCCAm6gAwIBAgIJAOKzFJgrn+rZMA0GCSqGSIb3DQEBCwUAMFcxCzAJBgNV\n" +
            "BAYTAlhYMRUwEwYDVQQHDAxEZWZhdWx0IENpdHkxHDAaBgNVBAoME0RlZmF1bHQg\n" +
            "Q29tcGFueSBMdGQxEzARBgNVBAMMCmNsaWVudHMtY2EwIBcNMTgwODIzMTYyMTI1\n" +
            "WhgPMjExODA3MzAxNjIxMjVaMFcxCzAJBgNVBAYTAlhYMRUwEwYDVQQHDAxEZWZh\n" +
            "dWx0IENpdHkxHDAaBgNVBAoME0RlZmF1bHQgQ29tcGFueSBMdGQxEzARBgNVBAMM\n" +
            "CmNsaWVudHMtY2EwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQCWws5F\n" +
            "EJOpfZ/sFJWYbYdJVxmZB+5PjCwA2TUZxF/3P4w/5g2KZaXNy89AfBC5vRDRgyDy\n" +
            "j/RwcDg80kDGKobcGhTx5YkWoNvR/2WuTN6KC8DM78bfEREDHDxiXfAXrMIi7Ux2\n" +
            "FvUX13l76Sp9kiG3ETLjFom3n/qhg1ITJqPJSJi3tey0o2Pd5Arv0MIhQyep++UR\n" +
            "tZfND5fgF5x7hgnSf9Q1P1dJnVadu+ohUmmG7g+zX4rTqjN2jmHcf9V4lLKdPGWw\n" +
            "LQEGnP9yDqlm8x12M/BcIJasRgcciVsKYFuXe09NEYBvUjW8L6gaQ6U9wcYZ2MlK\n" +
            "W/8LMGkSFfO4quAJAgMBAAGjUzBRMB0GA1UdDgQWBBQUwNmfsNj+PM240pVPxYx9\n" +
            "Q9eQhDAfBgNVHSMEGDAWgBQUwNmfsNj+PM240pVPxYx9Q9eQhDAPBgNVHRMBAf8E\n" +
            "BTADAQH/MA0GCSqGSIb3DQEBCwUAA4IBAQAnhbNKwMmnayHsT6kKgyyDV6RUUYs6\n" +
            "nYf3nx+GIQWSw4c5TOHDcTWdKpOxVnLNXYKQoSkb1RBoSMLBdQwidZ5K2DB5eXaG\n" +
            "rcfEbKNBc5ZCFgFEAyy35pitJOmU/KzCdKyvx+TR5hIgGoKajYX5JZxj+1rTPGKO\n" +
            "ePT9iFp1ZbzHjgw6vFeJ+D2ov6HfW6C/KuK9Y6xUpvRQLVjMJYCyzxkxQAxZvu/0\n" +
            "0HVYYH6UJ7kuWywFMWoBdZ8US/vuUSBYyCGNL9p6ol+h9rsz3cIWBVBjx8C3qKki\n" +
            "QtlIdmFljGSaGGY6aJjUvUdgoPp1yQPa5oS+afr5g9gaEp4lxP6mc+Li\n" +
            "-----END CERTIFICATE-----\n";

    private static final String CLIENTS_CERT_STORE_PASSWORD = "123456";

    private static final byte[] CLUSTER_CERT_STORE;
    private static final byte[] CLIENTS_CERT_STORE;

    static {
        InputStream is = MockCertManager.class.getClassLoader().getResourceAsStream("CLUSTER_CERT.str");
        CLUSTER_CERT_STORE = loadResource(is);
        is = MockCertManager.class.getClassLoader().getResourceAsStream("CLIENTS_CERT.str");
        CLIENTS_CERT_STORE = loadResource(is);
    }

    public static String clusterCaCert() {
        return Base64.getEncoder().encodeToString(CLUSTER_CERT.getBytes(Charset.defaultCharset()));
    }

    public static String clusterCaKey() {
        return Base64.getEncoder().encodeToString(CLUSTER_KEY.getBytes(Charset.defaultCharset()));
    }

    public static String clientsCaCert() {
        return Base64.getEncoder().encodeToString(CLIENTS_CERT.getBytes(Charset.defaultCharset()));
    }

    public static String clientsCaKey() {
        return Base64.getEncoder().encodeToString(CLIENTS_KEY.getBytes(Charset.defaultCharset()));
    }

    public static String clusterCaCertStore() {
        return Base64.getEncoder().encodeToString(CLUSTER_CERT_STORE);
    }

    public static String clientsCaCertStore() {
        return Base64.getEncoder().encodeToString(CLIENTS_CERT_STORE);
    }

    public static String clientsCaCertStorePassword() {
        return Base64.getEncoder().encodeToString(CLIENTS_CERT_STORE_PASSWORD.getBytes(Charset.defaultCharset()));
    }

    private void write(File keyFile, String str) throws IOException {
        try (FileWriter writer = new FileWriter(keyFile)) {
            writer.write(str);
        }
    }

    private static byte[] loadResource(InputStream is) {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        try {
            int read;
            byte[] data = new byte[2048];
            while ((read = is.read(data, 0, data.length)) != -1) {
                buffer.write(data, 0, read);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return buffer.toByteArray();
    }

    /**
     * Generate a self-signed certificate
     *
     * @param keyFile  path to the file which will contain the private key
     * @param certFile path to the file which will contain the self signed certificate
     * @param sbj      subject information
     * @param days     certificate duration
     * @throws IOException
     */
    @Override
    public void generateSelfSignedCert(File keyFile, File certFile, Subject sbj, int days) throws IOException {

        write(keyFile, CLUSTER_KEY);
        write(certFile, CLUSTER_CERT);
    }

    /**
     * Renew a new self-signed certificate, keeping the existing private key
     *
     * @param keyFile  path to the file containing the existing private key
     * @param certFile path to the file which will contain the new self signed certificate
     * @param sbj      subject information
     * @param days     certificate duration
     * @throws IOException
     */
    @Override
    public void renewSelfSignedCert(File keyFile, File certFile, Subject sbj, int days) throws IOException {
        generateSelfSignedCert(keyFile, certFile, sbj, days);
    }

    @Override
    public void generateRootCaCert(Subject subject, File subjectKeyFile, File subjectCertFile, ZonedDateTime notBefore, ZonedDateTime notAfter, int pathLength) throws IOException {
        generateSelfSignedCert(subjectKeyFile, subjectCertFile, subject, 1);
    }

    @Override
    public void generateIntermediateCaCert(File issuerCaKeyFile, File issuerCaCertFile, Subject subject, File subjectKeyFile, File subjectCertFile, ZonedDateTime notBefore, ZonedDateTime notAfter, int pathLength) throws IOException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void addCertToTrustStore(File certFile, String certAlias, File trustStoreFile, String trustStorePassword)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
        if (certFile.getName().contains("cluster")) {
            Files.write(trustStoreFile.toPath(), CLUSTER_CERT_STORE);
        } else if (certFile.getName().contains("clients")) {
            Files.write(trustStoreFile.toPath(), CLIENTS_CERT_STORE);
        }
    }

    @Override
    public void addKeyAndCertToKeyStore(File keyFile, File certFile, String alias, File keyStoreFile, String keyStorePassword) throws IOException {
        // never called during the tests which use this MockCertManager
        write(keyStoreFile, "key store");
    }

    @Override
    public void deleteFromTrustStore(List<String> aliases, File trustStoreFile, String trustStorePassword)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
        // never called during the tests which use this MockCertManager
    }

    /**
     * Generate a certificate sign request
     *
     * @param keyFile path to the file which will contain the private key
     * @param csrFile path to the file which will contain the certificate sign request
     * @param sbj     subject information
     */
    @Override
    public void generateCsr(File keyFile, File csrFile, Subject sbj) throws IOException {
        write(keyFile, "key file");
        write(csrFile, "csr file");
    }

    @Override
    public void generateCert(File csrFile, File caKey, File caCert, File crtFile, Subject sbj, int days) throws IOException {
        write(crtFile, "crt file");
    }

    @Override
    public void generateCert(File csrFile, byte[] caKey, byte[] caCert, File crtFile, Subject sbj, int days) throws IOException {
        write(crtFile, "crt file");
    }
}
