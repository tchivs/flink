/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.utils;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema.UnresolvedColumn;
import org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import io.confluent.flink.credentials.CredentialDecrypter;
import org.junit.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.security.KeyPair;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;

import static io.confluent.flink.table.modules.TestUtils.MockedInMemoryCredentialDecrypterImpl;
import static io.confluent.flink.table.modules.TestUtils.createKeyPair;
import static io.confluent.flink.table.modules.TestUtils.encryptMessage;
import static io.confluent.flink.table.modules.TestUtils.verifySignature;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Tests for MlUtils class. */
public class MlUtilsTest {
    private static final String PLAINTEXT = "plaintext";

    @ParameterizedTest
    @MethodSource("getTestTypes")
    public void testGetLogicalType(DataType resolvedType) {
        // This construct unresolveddatatype
        UnresolvedColumn column =
                new UnresolvedPhysicalColumn(
                        "name", DataTypes.of(resolvedType.getLogicalType().asSerializableString()));
        LogicalType parsedLogicalType = MlUtils.getLogicalType(column);
        assertThat(parsedLogicalType).isEqualTo(resolvedType.getLogicalType());
    }

    private static List<DataType> getTestTypes() {
        return Arrays.asList(
                DataTypes.NULL(),
                DataTypes.BOOLEAN(),
                DataTypes.TINYINT(),
                DataTypes.SMALLINT(),
                DataTypes.INT(),
                DataTypes.BIGINT(),
                DataTypes.FLOAT(),
                DataTypes.DOUBLE(),
                DataTypes.DATE(),
                DataTypes.BYTES(),
                DataTypes.STRING(),
                DataTypes.VARCHAR(3),
                DataTypes.CHAR(3),
                DataTypes.BINARY(3),
                DataTypes.VARBINARY(3),
                DataTypes.DECIMAL(10, 5),
                DataTypes.TIME(),
                DataTypes.TIME(3),
                DataTypes.TIMESTAMP(),
                DataTypes.TIMESTAMP(3),
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(),
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3),
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(),
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3),
                DataTypes.MAP(DataTypes.BIGINT(), DataTypes.INT()),
                DataTypes.MAP(DataTypes.CHAR(1), DataTypes.VARCHAR(1)),
                DataTypes.ARRAY(DataTypes.STRING()),
                DataTypes.MULTISET(DataTypes.STRING()),
                DataTypes.ROW(DataTypes.BIGINT(), DataTypes.ARRAY(DataTypes.INT())));
    }

    @Test
    public void testDecryptWithComputePoolKey() throws Exception {
        final KeyPair keyPair = createKeyPair();
        final String encryptedSecret =
                Base64.getEncoder().encodeToString(encryptMessage(PLAINTEXT, keyPair.getPublic()));
        final CredentialDecrypter decrypter =
                new MockedInMemoryCredentialDecrypterImpl(keyPair.getPrivate().getEncoded());
        assertThat(MlUtils.decryptSecretWithComputePoolKey(encryptedSecret, decrypter))
                .isEqualTo(PLAINTEXT);
    }

    @Test
    public void testSignData() throws Exception {
        final String data = "hello";
        final KeyPair keyPair = createKeyPair();
        final CredentialDecrypter decrypter =
                new MockedInMemoryCredentialDecrypterImpl(keyPair.getPrivate().getEncoded());
        final byte[] signature = MlUtils.signData(data, decrypter);
        assertThat(verifySignature(data, signature, keyPair.getPublic())).isTrue();
    }

    @Test
    public void testVerificationFailure() throws Exception {
        final String data = "hello";
        final KeyPair keyPair = createKeyPair();
        final CredentialDecrypter decrypter =
                new MockedInMemoryCredentialDecrypterImpl(keyPair.getPrivate().getEncoded());
        final byte[] signature = MlUtils.signData(data, decrypter);
        assertThat(verifySignature("bad data", signature, keyPair.getPublic())).isFalse();
    }
}
