/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.remoteudf.util;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.util.Preconditions;

import io.confluent.flink.table.catalog.CatalogInfo;
import io.confluent.flink.table.catalog.DatabaseInfo;
import io.confluent.flink.table.modules.remoteudf.mock.MockedCatalog;
import io.confluent.flink.table.modules.remoteudf.mock.MockedFunctionWithTypes;
import io.confluent.flink.table.service.ServiceTasks;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMWriter;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

import java.io.IOException;
import java.io.StringWriter;
import java.math.BigInteger;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.security.Security;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static io.confluent.flink.table.modules.remoteudf.RemoteUdfModule.CONFLUENT_CONFLUENT_REMOTE_UDF_APISERVER;
import static io.confluent.flink.table.modules.remoteudf.UdfUtil.FUNCTIONS_PREFIX;
import static io.confluent.flink.table.modules.remoteudf.UdfUtil.FUNCTION_ARGUMENT_TYPES_FIELD;
import static io.confluent.flink.table.modules.remoteudf.UdfUtil.FUNCTION_CATALOG_FIELD;
import static io.confluent.flink.table.modules.remoteudf.UdfUtil.FUNCTION_CLASS_NAME_FIELD;
import static io.confluent.flink.table.modules.remoteudf.UdfUtil.FUNCTION_DATABASE_FIELD;
import static io.confluent.flink.table.modules.remoteudf.UdfUtil.FUNCTION_ENV_FIELD;
import static io.confluent.flink.table.modules.remoteudf.UdfUtil.FUNCTION_NAME_FIELD;
import static io.confluent.flink.table.modules.remoteudf.UdfUtil.FUNCTION_ORG_FIELD;
import static io.confluent.flink.table.modules.remoteudf.UdfUtil.FUNCTION_RETURN_TYPE_FIELD;
import static io.confluent.flink.table.modules.remoteudf.UdfUtil.PLUGIN_ID_FIELD;
import static io.confluent.flink.table.modules.remoteudf.UdfUtil.PLUGIN_VERSION_ID_FIELD;
import static io.confluent.flink.table.service.ServiceTasks.INSTANCE;
import static io.confluent.flink.table.service.ServiceTasksOptions.CONFLUENT_REMOTE_UDF_ENABLED;

/** Test utility class with common functionality for Remote UDF testing. */
public class TestUtils {
    static final String GW_SERVER_TARGET = "localhost:50051";
    static final String APISERVER_TARGET = "http://localhost:8080";
    public static final int EXPECTED_INT_RETURN_VALUE = 424242;

    /** Generate a pair of public and private keys. */
    public static KeyPair createKeyPair() throws NoSuchAlgorithmException {
        KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
        kpg.initialize(2048);
        return kpg.generateKeyPair();
    }

    /** Converts input into a Base-64 encoded string (PEM format). */
    public static String convertToBase64PEM(Object value) throws IOException {
        StringWriter sw = new StringWriter();
        try (PEMWriter pw = new PEMWriter(sw)) {
            pw.writeObject(value);
        }
        return sw.toString();
    }

    /** Generates a self-signed X509 certificate. */
    public static Certificate createSelfSignedCertificate(KeyPair keyPair)
            throws OperatorCreationException, CertificateException, IOException {
        Provider bcProvider = new BouncyCastleProvider();
        Security.addProvider(bcProvider);

        Date startDate = new Date(System.currentTimeMillis());
        X500Name dnName = new X500Name("CN=localhost");
        // Use current timestamp as the certificate serial number
        BigInteger certSerialNumber = new BigInteger(Long.toString(startDate.getTime()));

        Calendar calendar = Calendar.getInstance();
        calendar.setTime(startDate);
        calendar.add(Calendar.YEAR, 1);

        Date endDate = calendar.getTime();
        // Same as the keyPair algorithm
        String signatureAlgorithm = "SHA256WithRSA";

        ContentSigner contentSigner =
                new JcaContentSignerBuilder(signatureAlgorithm).build(keyPair.getPrivate());

        JcaX509v3CertificateBuilder certBuilder =
                new JcaX509v3CertificateBuilder(
                        dnName, certSerialNumber, startDate, endDate, dnName, keyPair.getPublic());

        // Create basic self-signed certificate
        X509CertificateHolder holder = certBuilder.build(contentSigner);
        return new JcaX509CertificateConverter().getCertificate(holder);
    }

    public static Map<String, String> getBaseConfigMap() {
        return getBaseConfigMap(APISERVER_TARGET);
    }

    public static Map<String, String> getBaseConfigMap(String apiserverTarget) {
        Map<String, String> confMap = new HashMap<>();
        confMap.put(CONFLUENT_CONFLUENT_REMOTE_UDF_APISERVER.key(), apiserverTarget);
        return confMap;
    }

    public static TableEnvironment getSqlServiceTableEnvironment(
            MockedFunctionWithTypes[] testFunctions, boolean udfsEnabled, boolean createCatalog) {
        return getSqlServiceTableEnvironment(
                getBaseConfigMap(), testFunctions, udfsEnabled, createCatalog);
    }

    public static TableEnvironment getSqlServiceTableEnvironment(
            Map<String, String> confMap,
            MockedFunctionWithTypes[] testFunctions,
            boolean udfsEnabled,
            boolean createCatalog) {
        final TableEnvironment tableEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        if (createCatalog) {
            createCatalog(tableEnv);
        }
        if (udfsEnabled) {
            TestUtils.populateServiceTaskConfFromMockedFunctions(confMap, testFunctions);
            confMap.put(CONFLUENT_REMOTE_UDF_ENABLED.key(), String.valueOf(true));
        }
        INSTANCE.configureEnvironment(
                tableEnv, Collections.emptyMap(), confMap, ServiceTasks.Service.SQL_SERVICE);
        return tableEnv;
    }

    public static TableEnvironment getJssTableEnvironment(MockedFunctionWithTypes[] testFunctions) {
        return getJssTableEnvironment(getBaseConfigMap(), testFunctions);
    }

    public static TableEnvironment getJssTableEnvironment(
            Map<String, String> confMap, MockedFunctionWithTypes[] testFunctions) {
        final TableEnvironment tableEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        TestUtils.populateServiceTaskConfFromMockedFunctions(confMap, testFunctions);
        INSTANCE.configureEnvironment(
                tableEnv,
                Collections.emptyMap(),
                confMap,
                ServiceTasks.Service.JOB_SUBMISSION_SERVICE);
        return tableEnv;
    }

    private static void createCatalog(final TableEnvironment tableEnv) {
        tableEnv.registerCatalog(
                "cat1",
                new MockedCatalog(
                        CatalogInfo.of("env-1", "cat1"),
                        Collections.singletonList(DatabaseInfo.of("lkc-1", "db1"))));
    }

    /**
     * Populates UDF conf as used by the ServiceTasks module to load
     * ConfiguredRemoteScalarFunctions.
     */
    public static void populateServiceTaskConfFromMockedFunctions(
            Map<String, String> udfConf, MockedFunctionWithTypes[] funcs) {
        for (MockedFunctionWithTypes func : funcs) {
            Preconditions.checkState(func.getArgTypes().size() == func.getReturnType().size());
            udfConf.put(FUNCTIONS_PREFIX + func.getName() + "." + FUNCTION_ORG_FIELD, "test-org");
            udfConf.put(FUNCTIONS_PREFIX + func.getName() + "." + FUNCTION_ENV_FIELD, "test-env");
            udfConf.put(FUNCTIONS_PREFIX + func.getName() + "." + FUNCTION_CATALOG_FIELD, "cat1");
            udfConf.put(FUNCTIONS_PREFIX + func.getName() + "." + FUNCTION_DATABASE_FIELD, "db1");
            udfConf.put(
                    FUNCTIONS_PREFIX + func.getName() + "." + FUNCTION_NAME_FIELD, func.getName());
            udfConf.put(FUNCTIONS_PREFIX + func.getName() + "." + PLUGIN_ID_FIELD, "test-pluginId");
            udfConf.put(
                    FUNCTIONS_PREFIX + func.getName() + "." + PLUGIN_VERSION_ID_FIELD,
                    "test-versionId");
            udfConf.put(
                    FUNCTIONS_PREFIX + func.getName() + "." + FUNCTION_CLASS_NAME_FIELD,
                    "io.confluent.ExampleFunction");
            for (int i = 0; i < func.getArgTypes().size(); i++) {
                udfConf.put(
                        FUNCTIONS_PREFIX
                                + func.getName()
                                + "."
                                + FUNCTION_ARGUMENT_TYPES_FIELD
                                + "."
                                + i,
                        String.join(";", func.getArgTypes().get(i)));
                udfConf.put(
                        FUNCTIONS_PREFIX
                                + func.getName()
                                + "."
                                + FUNCTION_RETURN_TYPE_FIELD
                                + "."
                                + i,
                        func.getReturnType().get(i));
            }
        }
    }
}
