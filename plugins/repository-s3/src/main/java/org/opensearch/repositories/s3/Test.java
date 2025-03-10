/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3;


import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.encryption.s3.S3AsyncEncryptionClient;
import software.amazon.encryption.s3.materials.CryptographicMaterialsManager;
import software.amazon.encryption.s3.materials.DefaultCryptoMaterialsManager;
import software.amazon.encryption.s3.materials.KmsKeyring;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.CRC32;

public class Test {

    private static final String BUCKET = "test-bucket-crt";
    private static final String OBJECT_KEY = "test-object";
    private static final String AWS_KMS_KEY_ID = "7e4043c0-008a-4fb3-92fe-eb1f2fdf0316";
    private static final String FILE_TO_UPLOAD = "/Users/bukhtawa/Desktop/th1";
    private static final Region REGION = Region.US_WEST_2;

    public static S3AsyncClient createEncryptionClient() {
        // Create KMS client
        KmsClient kmsClient = KmsClient.builder()
            .region(REGION)  // Replace with your desired region
            .credentialsProvider(getCredentialProvider())
            .build();

        KmsKeyring kmsKeyring = KmsKeyring.builder()
            .kmsClient(kmsClient)
            .wrappingKeyId(AWS_KMS_KEY_ID)
            .build();

        CryptographicMaterialsManager cmm = DefaultCryptoMaterialsManager.builder()
            .keyring(kmsKeyring)
            .build();
        // Create S3Crt client
        return S3AsyncEncryptionClient.builder()
            .enableLegacyUnauthenticatedModes(true)
            .cryptoMaterialsManager(cmm)
            .wrappedClient(S3AsyncClient.crtBuilder()
                .credentialsProvider(getCredentialProvider())
                .region(REGION)
                .build()
            )
            .credentialsProvider(getCredentialProvider())
            .region(REGION)
            .build();
    }

    private static S3AsyncClient createSimpleClient() {
        return S3AsyncClient.crtBuilder()
            .credentialsProvider(getCredentialProvider())
            .region(REGION)
            .build();
    }

    public static void main(String[] args) {
        AtomicReference<String> checksumUnencryptedUpload = new AtomicReference<>();
        try (S3AsyncClient asyncClient = createSimpleClient()) {
            asyncClient.putObject(PutObjectRequest.builder()
                    .bucket(BUCKET)
                    .key(OBJECT_KEY)
                    .checksumAlgorithm(ChecksumAlgorithm.CRC32)
                    .build(),
                AsyncRequestBody.fromFile(Path.of(FILE_TO_UPLOAD))).whenComplete((response, error) -> {
                if (error != null) {
                    System.err.println("Error: " + error.getMessage());
                } else {
                    checksumUnencryptedUpload.set(response.checksumCRC32());
                    System.out.println("Success!" + response);
                }
            }).join();
        } catch (Exception e) {
        System.err.println("Error creating S3 client: " + e.getMessage());
    }

       try (S3AsyncClient asyncClient = createEncryptionClient()) {
            asyncClient.putObject(PutObjectRequest.builder()
                .bucket(BUCKET)
                .key(OBJECT_KEY)
                .checksumCRC32(checksumUnencryptedUpload.get())
                .build(),
                AsyncRequestBody.fromFile(Path.of(FILE_TO_UPLOAD))).whenComplete((response, error) -> {
                if (error != null) {
                    System.err.println("Error: " + error.getMessage());
                } else {
                    System.out.println("Success!" + response);
                }
            }).join();


            // Use the encryption client for S3 operations
            // The client will automatically handle encryption/decryption
        } catch (Exception e) {
            System.err.println("Error creating S3 encryption client: " + e.getMessage());
        }

       System.out.println("CRC32 checksum: " + calculateCRC32(FILE_TO_UPLOAD));
    }

    public static String calculateCRC32(String filePath)  {
        CRC32 crc = new CRC32();
        try (FileInputStream fis = new FileInputStream(filePath)) {
            byte[] buffer = new byte[8192]; // Read in chunks
            int bytesRead;
            while ((bytesRead = fis.read(buffer)) != -1) {
                crc.update(buffer, 0, bytesRead);
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        long crcValue = crc.getValue();
        byte[] crcBytes = new byte[4];
        crcBytes[0] = (byte) (crcValue & 0xFF);
        crcBytes[1] = (byte) ((crcValue >> 8) & 0xFF);
        crcBytes[2] = (byte) ((crcValue >> 16) & 0xFF);
        crcBytes[3] = (byte) ((crcValue >> 24) & 0xFF);
        String checksum = Base64.getEncoder().encodeToString(crcBytes);
        System.out.println("CRC32 checksum: " + checksum);
        return checksum;
    }

    private static AwsCredentialsProvider getCredentialProvider() {
        return ProfileCredentialsProvider.create("dev");
    }

}

