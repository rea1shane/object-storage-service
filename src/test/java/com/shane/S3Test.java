package com.shane;

import org.junit.Test;

public class S3Test {

    final static S3 s3Instance = new S3();

    String bucketName = "create-by-java-sdk1";

    /**
     * <p>
     * {@link S3#checkBucketExist(String)}
     * </p>
     * <p>
     * {@link S3#createBucket(String)}
     * </p>
     * <p>
     * {@link S3#enableBucketVersioning(String)}
     * </p>
     */
    @Test
    public void testInitBucket() {
        System.out.printf("check bucket [%s]...\n", bucketName);
        boolean exist = s3Instance.checkBucketExist(bucketName);
        if (exist) {
            // TODO 检测 bucket 归属权
            System.out.println("bucket already exist, skip init");
            return;
        }
        System.out.println("bucket does not exist");
        System.out.printf("start init bucket [%s]\n", bucketName);
        System.out.println("> create bucket...");
        boolean r1 = s3Instance.createBucket(bucketName);
        if (!r1) {
            System.out.println("> create bucket failure");
            rollbackInitBucket(bucketName, false);
            return;
        }
        System.out.println("> create bucket success");
        System.out.println("> enable versioning...");
        boolean r2 = s3Instance.enableBucketVersioning(bucketName);
        if (!r2) {
            System.out.println("> enable versioning failure");
            rollbackInitBucket(bucketName, true);
            return;
        }
        System.out.println("> enable versioning success");
        System.out.println("init bucket success");
    }

    /**
     * <p>
     * {@link S3#deleteBucket(String)}
     * </p>
     *
     * @param bucketName    桶的名称
     * @param bucketCreated 桶是否已经创建
     */
    private void rollbackInitBucket(String bucketName, boolean bucketCreated) {
        System.out.println("init bucket failure");
        System.out.println("start rollback");
        if (bucketCreated) {
            System.out.println("> delete bucket...");
            boolean r = s3Instance.deleteBucket(bucketName);
            if (!r) {
                System.out.println("> delete bucket failure");
                System.out.println("rollback failure");
                return;
            }
            System.out.println("> delete bucket success");
        }
        System.out.println("rollback success");
    }

}
