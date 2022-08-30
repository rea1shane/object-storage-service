package com.shane;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.BucketVersioningConfiguration;
import com.amazonaws.services.s3.model.SetBucketVersioningConfigurationRequest;

public class S3 {

    private AmazonS3 s3;

    /**
     * <p>
     * 使用默认配置的 S3 client
     * </p>
     */
    public S3() {
        setS3(null);
    }

    /**
     * <p>
     * 使用自定义的 s3 client
     * </p>
     *
     * @param s3 自定义 s3 client
     */
    public S3(AmazonS3 s3) {
        setS3(s3);
    }

    private void setS3(AmazonS3 s3) {
        this.s3 = s3 == null ? AmazonS3ClientBuilder.standard().build() : s3;
    }

    /**
     * <p>
     * 检查桶名是否存在
     * </p>
     *
     * @param bucketName 桶的名称
     * @return true：存在 / false：不存在
     */
    public boolean checkBucketExist(String bucketName) {
        return this.s3.doesBucketExistV2(bucketName);
    }

    /**
     * <p>
     * 创建桶
     * </p>
     *
     * @param bucketName 桶的名称
     * @return 操作结果
     */
    public boolean createBucket(String bucketName) {
        try {
            this.s3.createBucket(bucketName);
        } catch (Exception e) {
            System.err.printf("error create bucket [%s]: %s", bucketName, e);
            return false;
        }
        return true;
    }

    /**
     * <p>
     * 开启桶的版本控制
     * </p>
     *
     * @param bucketName 桶的名称
     * @return 操作结果
     */
    public boolean enableBucketVersioning(String bucketName) {
        try {
            BucketVersioningConfiguration configuration = new BucketVersioningConfiguration(BucketVersioningConfiguration.ENABLED);
            SetBucketVersioningConfigurationRequest request = new SetBucketVersioningConfigurationRequest(bucketName, configuration);
            this.s3.setBucketVersioningConfiguration(request);
        } catch (Exception e) {
            System.err.printf("error enable versioning for bucket [%s]: %s", bucketName, e);
            return false;
        }
        return true;
    }

    /**
     * <p>
     * 删除桶
     * </p>
     *
     * @param bucketName 桶的名称
     * @return 操作结果
     */
    public boolean deleteBucket(String bucketName) {
        try {
            this.s3.deleteBucket(bucketName);
        } catch (Exception e) {
            System.err.printf("error delete bucket [%s]: %s", bucketName, e);
            return false;
        }
        return true;
    }

}
