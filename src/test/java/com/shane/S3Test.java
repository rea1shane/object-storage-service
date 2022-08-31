package com.shane;

import com.amazonaws.auth.policy.Policy;
import org.junit.Test;

public class S3Test {

    final static ObjectStorageService OSS = new ObjectStorageService();

    String bucketName = "create-by-java-sdk-new";

    /**
     * <p>
     * {@link ObjectStorageService#checkBucketExist(String)}
     * </p>
     * <p>
     * {@link ObjectStorageService#createBucket(String)}
     * </p>
     * <p>
     * {@link ObjectStorageService#enableBucketVersioning(String)}
     * </p>
     */
    @Test
    public void testInitBucket() {
        System.out.printf("check bucket [%s]...\n", bucketName);
        boolean exist = OSS.checkBucketExist(bucketName);
        if (exist) {
            // TODO 检测 bucket 归属权
            System.out.println("bucket already exist, skip init");
            return;
        }
        System.out.println("bucket does not exist");
        System.out.printf("start init bucket [%s]\n", bucketName);
        System.out.println("> create bucket...");
        boolean r1 = OSS.createBucket(bucketName);
        if (!r1) {
            System.out.println("> create bucket failure");
            rollbackInitBucket(bucketName, false);
            return;
        }
        System.out.println("> create bucket success");
        System.out.println("> enable versioning...");
        boolean r2 = OSS.enableBucketVersioning(bucketName);
        if (!r2) {
            System.out.println("> enable versioning failure");
            rollbackInitBucket(bucketName, true);
            return;
        }
        System.out.println("> enable versioning success");
        System.out.println("> init policy...");
        boolean r3 = OSS.setBucketPolicy(bucketName, new Policy()
                .withStatements(OSS.generateDenyAllActionsStatement(bucketName))
        );
        if (!r3) {
            System.out.println("> init policy failure");
            rollbackInitBucket(bucketName, true);
            return;
        }
        System.out.println("> init policy success");
        System.out.println("init bucket success");
    }

    /**
     * <p>
     * {@link ObjectStorageService#deleteBucket(String)}
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
            boolean r = OSS.deleteBucket(bucketName);
            if (!r) {
                System.out.println("> delete bucket failure");
                System.out.println("rollback failure");
                return;
            }
            System.out.println("> delete bucket success");
        }
        System.out.println("rollback success");
    }

    @Test
    public void testFunc() {
        System.out.println(new Policy().withStatements(OSS.generateDenyAllActionsStatement(bucketName)).toJson());
    }

}
