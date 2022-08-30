package com.shane;

import com.amazonaws.services.s3.model.Bucket;
import org.junit.Test;

import java.util.List;

public class S3Test {

    final static S3 s3Instance = new S3();

    String sourceBucketName = "create-by-java-sdk";

    /**
     * <p>
     * {@link S3#listBuckets()}
     * </p>
     */
    @Test
    public void testListBuckets() {
        List<Bucket> buckets = s3Instance.listBuckets();
        for (Bucket b : buckets) {
            System.out.println(b.getName());
        }
    }

    /**
     * <p>
     * {@link S3#checkBucketExist(String)}
     * </p>
     */
    @Test
    public void testCheckBucketExist() {
        System.out.println(s3Instance.checkBucketExist(sourceBucketName));
    }

    /**
     * <p>
     * {@link S3#getBucket(String)}
     * </p>
     */
    @Test
    public void testGetBucket() {
        System.out.println(s3Instance.getBucket(sourceBucketName));
    }

    /**
     * <p>
     * {@link S3#createBucket(String)}
     * </p>
     */
    @Test
    public void testCreateBucket() {
        System.out.println(s3Instance.createBucket(sourceBucketName));
    }

}
