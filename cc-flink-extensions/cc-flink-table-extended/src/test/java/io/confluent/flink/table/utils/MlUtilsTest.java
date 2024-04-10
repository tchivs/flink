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
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Tests for MlUtils class. */
public class MlUtilsTest {

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
    public void testDecryptPlaintext() {
        final String plainText = "plainText";
        assertThat(MlUtils.decryptSecret(plainText, true)).isEqualTo(plainText);
    }

    @Test
    public void testEmptySecret() {
        final String emptySecret = "";
        assertThat(MlUtils.decryptSecret(emptySecret, true)).isEqualTo(emptySecret);
        assertThat(MlUtils.decryptSecret(emptySecret, false)).isEqualTo(emptySecret);
    }

    @Test
    public void testDecryptWithComputePoolKey() throws Exception {
        final String plainText = "plaintext";
        final KeyPair keyPair = createKeyPair();
        final String encryptedSecret =
                Base64.getEncoder().encodeToString(encryptMessage(plainText, keyPair.getPublic()));
        final CredentialDecrypter decrypter =
                new MockedInMemoryCredentialDecrypterImpl(keyPair.getPrivate().getEncoded());
        assertThat(MlUtils.decryptSecretWithComputePoolKey(encryptedSecret, decrypter))
                .isEqualTo(plainText);
    }
}
