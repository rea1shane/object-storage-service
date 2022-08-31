package com.shane;

import com.amazonaws.auth.policy.Policy;
import com.amazonaws.auth.policy.Principal;
import com.amazonaws.auth.policy.Resource;
import com.amazonaws.auth.policy.Statement;
import com.amazonaws.auth.policy.actions.S3Actions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.BucketPolicy;
import com.amazonaws.services.s3.model.BucketVersioningConfiguration;
import com.amazonaws.services.s3.model.SetBucketVersioningConfigurationRequest;

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

    /**
     * <p>
     * 生成对桶无权限的 statement
     * </p>
     * <p>
     * statement id: DenyAllActions
     * </p>
     *
     * @param bucketName 桶的名称
     * @return statement
     */
    // TODO ARN
    public Statement generateDenyAllActionsStatement(String bucketARN) {
        Statement statement = new Statement(Statement.Effect.Deny)
                .withActions(S3Actions.AllS3Actions)
                .withPrincipals(Principal.All)
                .withResources(new Resource(bucketARN + "/*"));
        statement.setId("DenyAllActions");
        return statement;
    }

    /**
     * <p>
     * 生成具有对桶路径全部操作权限的 statement
     * </p>
     * <p>
     * statement id: Allow{sid}AllActions
     * </p>
     *
     * @param bucketName 桶的名称
     * @param path       路径，以斜杠 / 开头
     * @param accountId  用户 id
     * @param sid        statement id，驼峰命名法，首字母大写
     * @return statement
     */
    // TODO ARN
    public Statement generateAllowAllActionsStatement(String bucketARN, String path, String accountId, String sid) {
        Statement statement = new Statement(Statement.Effect.Allow)
                .withActions(S3Actions.AllS3Actions)
                .withPrincipals(new Principal(accountId))
                .withResources(new Resource(bucketARN + path));
        statement.setId(sid + "AllowAllActions");
        return statement;
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
            policy = Policy.fromJson(bucketPolicy.getPolicyText());
        } catch (Exception e) {
            System.err.printf("error get policy from bucket [%s]: %s", bucketName, e);
        }
        return policy;
    }

}
