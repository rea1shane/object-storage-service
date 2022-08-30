package com.shane;

import org.junit.Test;

public class S3Test {

    final static S3 s3Instance = new S3();

    String bucketName = "create-by-java-sdk1";

    /**
     * <p>
     * {@link S3#checkBucketExist(String)}
     * </p>
     */
    @Test
    public void testCheckBucketExist() {
        boolean exist = s3Instance.checkBucketExist(bucketName);
        System.out.println(exist);
    }

    /**
     * <p>
     * {@link S3#createBucket(String)}
     * </p>
     */
    @Test
    public void testCreateBucket() {
        boolean result = s3Instance.createBucket(bucketName);
        System.out.println(result);
    }

    /**
     * <p>
     * {@link S3#enableBucketVersioning(String)}
     * </p>
     */
    @Test
    public void testGetBucketLifecycleConfiguration() {
        boolean result = s3Instance.enableBucketVersioning(bucketName);
        System.out.println(result);
    }

}
