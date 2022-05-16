package io.deephaven.server.config;

import io.deephaven.UncheckedDeephavenException;
import io.grpc.util.CertificateUtils;
import nl.altindag.ssl.SSLFactory;
import nl.altindag.ssl.exception.GenericKeyStoreException;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;

/**
 * Package private - nl.altindag.ssl is an implementation detail.
 */
class DeephavenSslUtils {
    static SSLFactory create(SSLConfig config) {
        final SSLFactory.Builder builder = SSLFactory.builder();
        addIdentity(builder, config.identity());
        addTrust(builder, config);
        return builder.build();
    }

    private static void addTrust(SSLFactory.Builder builder, SSLConfig config) {
        if (config.withJDKTrust()) {
            builder.withDefaultTrustMaterial();
        }
        if (config.withSystemPropertyTrust()) {
            builder.withSystemPropertyDerivedTrustMaterial();
        }
        if (config.withTrustAll()) {
            builder.withTrustingAllCertificatesWithoutValidation();
        }
        for (TrustConfig trust : config.trust()) {
            addTrust(builder, trust);
        }
    }

    private static void addTrust(SSLFactory.Builder builder, TrustConfig config) {
        config.walk(new TrustConfig.Visitor<Void>() {
            @Override
            public Void visit(TrustStoreConfig trustStore) {
                addTrust(builder, trustStore);
                return null;
            }

            @Override
            public Void visit(TrustCertificatesConfig certificates) {
                addTrust(builder, certificates);
                return null;
            }
        });
    }

    private static void addTrust(SSLFactory.Builder builder, TrustCertificatesConfig config) {
        try {
            final X509Certificate[] x509Certificates = readX509Certificates(Path.of(config.path()));
            builder.withTrustMaterial(x509Certificates);
        } catch (GenericKeyStoreException e) {
            throw new UncheckedDeephavenException(e.getCause());
        } catch (CertificateException | IOException e) {
            throw new UncheckedDeephavenException(e);
        }
    }

    private static void addTrust(SSLFactory.Builder builder, TrustStoreConfig config) {
        try {
            final char[] password = config.password().toCharArray();
            builder.withTrustMaterial(Path.of(config.path()), password);
        } catch (GenericKeyStoreException e) {
            throw new UncheckedDeephavenException(e.getCause());
        }
    }

    private static void addIdentity(SSLFactory.Builder builder, IdentityConfig config) {
        config.walk(new IdentityConfig.Visitor<Void>() {
            @Override
            public Void visit(KeyStoreConfig keyStore) {
                addIdentity(builder, keyStore);
                return null;
            }

            @Override
            public Void visit(PrivateKeyConfig privateKey) {
                addIdentity(builder, privateKey);
                return null;
            }
        });
    }

    private static void addIdentity(SSLFactory.Builder builder, KeyStoreConfig config) {
        final char[] password = config.password().toCharArray();
        try {
            if (config.keystoreType().isPresent()) {
                builder.withIdentityMaterial(Path.of(config.path()), password, config.keystoreType().get());
            } else {
                builder.withIdentityMaterial(Path.of(config.path()), password);
            }
        } catch (GenericKeyStoreException e) {
            throw new UncheckedDeephavenException(e.getCause());
        }
    }

    private static void addIdentity(SSLFactory.Builder builder, PrivateKeyConfig config) {
        try {
            final PrivateKey privateKey = readPrivateKey(Paths.get(config.privateKeyPath()));
            final X509Certificate[] x509Certificates = readX509Certificates(Paths.get(config.certChainPath()));
            final char[] password = config.privateKeyPassword().map(String::toCharArray).orElse(null);
            final String alias = config.alias().orElse(null);
            builder.withIdentityMaterial(privateKey, password, alias, x509Certificates);
        } catch (GenericKeyStoreException e) {
            throw new UncheckedDeephavenException(e.getCause());
        } catch (CertificateException | IOException | NoSuchAlgorithmException | InvalidKeySpecException e) {
            throw new UncheckedDeephavenException(e);
        }
    }

    private static PrivateKey readPrivateKey(Path path)
            throws IOException, NoSuchAlgorithmException, InvalidKeySpecException {
        try (final InputStream in = Files.newInputStream(path)) {
            return CertificateUtils.getPrivateKey(in);
        }
    }

    private static X509Certificate[] readX509Certificates(Path path) throws IOException, CertificateException {
        try (final InputStream in = Files.newInputStream(path)) {
            return CertificateUtils.getX509Certificates(in);
        }
    }
}
