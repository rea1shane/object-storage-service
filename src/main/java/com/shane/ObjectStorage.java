package com.shane;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import lombok.Builder;

import java.security.InvalidParameterException;

public class ObjectStorage {

    // TODO 通过读取配置文件或者传参
    private String objectStorageKind = "s3";
    private String objectStorageRegion = "cn-north-1";
    private String objectStorageEndpoint = "";
    // 存储桶命名规则 https://docs.amazonaws.cn/AmazonS3/latest/userguide/bucketnamingrules.html
    private String bucketName = "aml-bucket";

    /**
     * <p>
     * 创建一个 s3 客户端用于对对象存储进行操作，
     * </p>
     *
     * @param token 对象存储身份令牌
     * @return s3 客户端
     */
    protected AmazonS3 createClient(Token token) {
        AmazonS3 client;

        AWSCredentials awsCredentials = new BasicAWSCredentials(token.accessKey, token.secretKey);
        AWSCredentialsProvider awsCredentialsProvider = new AWSStaticCredentialsProvider(awsCredentials);

        AmazonS3ClientBuilder amazonS3ClientBuilder = AmazonS3ClientBuilder
                .standard()
                .withCredentials(awsCredentialsProvider);

        switch (objectStorageKind) {
            case "s3":
                client = amazonS3ClientBuilder
                        .withRegion(objectStorageRegion)
                        .build();
                break;
            case "ceph":
                AwsClientBuilder.EndpointConfiguration endpointConfiguration = new AwsClientBuilder.EndpointConfiguration(objectStorageEndpoint, objectStorageRegion);
                client = amazonS3ClientBuilder
                        .withEndpointConfiguration(endpointConfiguration)
                        .build();
                break;
            default:
                throw new InvalidParameterException("Error param object-storage.kind in spring properties, should in [ s3, ceph ] but" + objectStorageKind);
        }

        return client;
    }

    /**
     * <p>
     * 获取桶的名称，该场景中整个应用只使用一个桶
     * </p>
     *
     * @return 桶的名称
     */
    protected String getBucketName() {
        return bucketName;
    }

    /**
     * <p>
     * 对象存储身份令牌
     * </p>
     */
    @Builder
    public static class Token {
        private String accessKey;
        private String secretKey;
    }

}
