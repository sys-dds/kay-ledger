package com.kayledger.api.reporting.application;

import java.net.URI;

import org.springframework.stereotype.Service;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

@Service
public class ObjectStorageService {

    private final ObjectStorageProperties properties;
    private final S3Client s3Client;

    public ObjectStorageService(ObjectStorageProperties properties) {
        this.properties = properties;
        this.s3Client = S3Client.builder()
                .endpointOverride(URI.create(properties.getEndpoint()))
                .region(Region.of(properties.getRegion()))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(properties.getAccessKey(), properties.getSecretKey())))
                .forcePathStyle(properties.isPathStyleAccessEnabled())
                .build();
    }

    public void put(String storageKey, String contentType, byte[] content) {
        ensureBucket();
        s3Client.putObject(PutObjectRequest.builder()
                .bucket(properties.getBucket())
                .key(storageKey)
                .contentType(contentType)
                .contentLength((long) content.length)
                .build(), RequestBody.fromBytes(content));
    }

    private void ensureBucket() {
        try {
            headBucket();
        } catch (NoSuchBucketException exception) {
            createBucket();
        } catch (S3Exception exception) {
            if (isMissingBucket(exception)) {
                createBucket();
                return;
            }
            throw exception;
        }
    }

    private void headBucket() {
        s3Client.headBucket(HeadBucketRequest.builder().bucket(properties.getBucket()).build());
    }

    private void createBucket() {
        try {
            s3Client.createBucket(CreateBucketRequest.builder().bucket(properties.getBucket()).build());
        } catch (S3Exception exception) {
            if (!bucketAlreadyAvailable(exception)) {
                throw exception;
            }
        }
        headBucket();
    }

    private static boolean isMissingBucket(S3Exception exception) {
        String errorCode = exception.awsErrorDetails() == null ? null : exception.awsErrorDetails().errorCode();
        return exception.statusCode() == 404
                || "NoSuchBucket".equals(errorCode)
                || "NotFound".equals(errorCode);
    }

    private static boolean bucketAlreadyAvailable(S3Exception exception) {
        String errorCode = exception.awsErrorDetails() == null ? null : exception.awsErrorDetails().errorCode();
        return exception.statusCode() == 409
                || "BucketAlreadyOwnedByYou".equals(errorCode)
                || "BucketAlreadyExists".equals(errorCode);
    }
}
