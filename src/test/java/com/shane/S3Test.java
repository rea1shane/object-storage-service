package com.shane;

import org.junit.Test;

public class S3Test {

    final static S3 s3Instance = new S3();

    String sourceBucketName = "create-by-java-sdk";

    /**
     * <p>
     * {@link S3#checkBucketExist(String)}
     * </p>
     */
    @Test
    public void testCheckBucketExist() {
        boolean exist = s3Instance.checkBucketExist(sourceBucketName);
        System.out.println(exist);
    }

    /**
     * <p>
     * {@link S3#createBucket(String)}
     * </p>
     */
    @Test
    public void testCreateBucket() {
        boolean success = s3Instance.createBucket(sourceBucketName);
        System.out.println(success);
    }

}
