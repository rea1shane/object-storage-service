package com.shane;

import com.amazonaws.auth.policy.Policy;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.BucketPolicy;
import com.amazonaws.services.s3.model.BucketVersioningConfiguration;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.Region;
import com.amazonaws.services.s3.model.SetBucketVersioningConfigurationRequest;
import com.amazonaws.services.s3.model.ownership.ObjectOwnership;

import java.util.List;

public class ObjectStorageService {

    private AmazonS3 s3;

    /**
     * <p>
     * 使用默认配置的 S3 client
     * </p>
     */
    public ObjectStorageService() {
        setS3(null);
    }

    /**
     * <p>
     * 使用自定义的 S3 client
     * </p>
     *
     * @param s3 自定义 s3 client
     */
    public ObjectStorageService(AmazonS3 s3) {
        setS3(s3);
    }

    private void setS3(AmazonS3 s3) {
        this.s3 = s3 == null ? AmazonS3ClientBuilder.standard().build() : s3;
    }

    public Region getRegion() {
        return s3.getRegion();
    }

    /**
     * <p>
     * 列出所有的桶
     * </p>
     *
     * @return 桶列表
     */
    private List<Bucket> listBuckets() {
        return s3.listBuckets();
    }

    /**
     * <p>
     * 获取桶
     * </p>
     *
     * @param bucketName 桶的名称
     * @return 指定名称的桶，没有的话返回 null
     */
    // TODO 用于校验桶的持有者
    public Bucket getBucket(String bucketName) {
        Bucket bucket = null;
        List<Bucket> buckets = listBuckets();
        for (Bucket b : buckets) {
            if (b.getName().equals(bucketName)) {
                bucket = b;
            }
        }
        return bucket;
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
        return s3.doesBucketExistV2(bucketName);
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
            s3.createBucket(new CreateBucketRequest(bucketName)
                    .withCannedAcl(CannedAccessControlList.Private)
                    .withObjectOwnership(ObjectOwnership.BucketOwnerEnforced));
        } catch (Exception e) {
            System.err.printf("error create bucket [%s]: %s", bucketName, e);
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
            s3.deleteBucket(bucketName);
        } catch (Exception e) {
            System.err.printf("error delete bucket [%s]: %s", bucketName, e);
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
            s3.setBucketVersioningConfiguration(request);
        } catch (Exception e) {
            System.err.printf("error enable versioning for bucket [%s]: %s", bucketName, e);
            return false;
        }
        return true;
    }

    /**
     * <p>
     * 设置桶策略
     * </p>
     *
     * @param bucketName 桶的名称
     * @param policy     桶的策略
     * @return 操作结果
     */
    public boolean setBucketPolicy(String bucketName, Policy policy) {
        try {
            s3.setBucketPolicy(bucketName, policy.toJson());
        } catch (Exception e) {
            System.err.printf("error set policy for bucket [%s]: %s", bucketName, e);
            return false;
        }
        return true;
    }

    /**
     * <p>
     * 获取桶策略
     * </p>
     *
     * @param bucketName 桶的名称
     * @return 桶的策略
     */
    public Policy getBucketPolicy(String bucketName) {
        Policy policy = null;
        try {
            BucketPolicy bucketPolicy = s3.getBucketPolicy(bucketName);
            String policyText = bucketPolicy.getPolicyText();
            policy = policyText == null ? new Policy() : Policy.fromJson(policyText);
        } catch (Exception e) {
            System.err.printf("error get policy from bucket [%s]: %s", bucketName, e);
        }
        return policy;
    }

}
