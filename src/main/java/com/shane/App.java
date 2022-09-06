package com.shane;

import com.amazonaws.auth.policy.Policy;
import com.amazonaws.auth.policy.Principal;
import com.amazonaws.auth.policy.Resource;
import com.amazonaws.auth.policy.Statement;
import com.amazonaws.auth.policy.actions.S3Actions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.Region;

import java.util.List;

public class App {

    // TODO 通过读取配置文件或者传参
    // 存储桶命名规则 https://docs.amazonaws.cn/AmazonS3/latest/userguide/bucketnamingrules.html
    private static final String BUCKET_NAME = "aml-bucket";

    private ObjectStorageService oss;
    private String arnId;

    public App() {
        init(null);
    }

    public App(AmazonS3 s3) {
        init(s3);
    }

    private void init(AmazonS3 s3) {
        oss = new ObjectStorageService(s3);
        Region region = oss.getRegion();
        switch (region) {
            case CN_Northwest_1:
            case CN_Beijing:
                arnId = "arn:aws-cn";
                break;
            default:
                arnId = "arn:aws";
                break;
        }
    }

    /**
     * <p>
     * 用于系统配置对象存储时初始化对象存储
     * </p>
     *
     * @return 操作结果
     */
    public boolean initBucket() {
        System.out.printf("start init bucket [%s]\n", BUCKET_NAME);
        System.out.println("check bucket exist...");
        boolean exist = oss.checkBucketExist(BUCKET_NAME);
        if (exist) {
            // TODO 检测 bucket 归属权
            // TODO 检查 bucket 设置
            System.out.println("bucket already exist, skip init");
            return true;
        }
        System.out.println("bucket does not exist");
        System.out.println("> create bucket...");
        boolean r1 = oss.createBucket(BUCKET_NAME);
        if (!r1) {
            System.out.println("> create bucket failure");
            rollbackInitBucket(false);
            return false;
        }
        System.out.println("> create bucket success");
        System.out.println("> enable versioning...");
        boolean r2 = oss.enableBucketVersioning(BUCKET_NAME);
        if (!r2) {
            System.out.println("> enable versioning failure");
            rollbackInitBucket(true);
            return false;
        }
        System.out.println("> enable versioning success");
        System.out.println("> init policy...");
        boolean r3 = oss.setBucketPolicy(BUCKET_NAME, getInitialPolicy());
        if (!r3) {
            System.out.println("> init policy failure");
            rollbackInitBucket(true);
            return false;
        }
        System.out.println("> init policy success");
        System.out.println("init bucket success");
        return true;
    }

    /**
     * <p>
     * {@link ObjectStorageService#deleteBucket(String)}
     * </p>
     *
     * @param bucketCreated 桶是否已经创建
     */
    private void rollbackInitBucket(boolean bucketCreated) {
        System.out.println("init bucket failure");
        System.out.println("start rollback");
        if (bucketCreated) {
            System.out.println("> delete bucket...");
            boolean r = oss.deleteBucket(BUCKET_NAME);
            if (!r) {
                System.out.println("> delete bucket failure");
                System.out.println("rollback failure");
                return;
            }
            System.out.println("> delete bucket success");
        }
        System.out.println("rollback success");
    }

    /**
     * <p>
     * 获取初始 policy
     * </p>
     *
     * @return 初始 policy
     */
    private Policy getInitialPolicy() {
        return new Policy()
                .withStatements(generateDenyAllActionsStatement());
    }

    /**
     * <p>
     * 生成组织所有行为的 statement，sid 为 DenyAll
     * </p>
     *
     * @return deny all actions statement
     */
    private Statement generateDenyAllActionsStatement() {
        Statement statement = new Statement(Statement.Effect.Deny)
                .withActions(S3Actions.AllS3Actions)
                .withPrincipals(Principal.All)
                .withResources(new Resource(arnId + ":s3:::" + BUCKET_NAME + "/*"));
        statement.setId(BUCKET_NAME + "-DenyAll");
        return statement;
    }

    /**
     * <p>
     * 生成具有对桶路径全部操作权限的 statement
     * </p>
     *
     * @param sid        statement id
     * @param path       路径，以斜杠 / 开头
     * @param accountIds 用户 id
     * @return statement
     */
    private Statement generateAllowAllActionsStatement(String sid, String path, List<String> accountIds) {
        Statement statement = new Statement(Statement.Effect.Allow)
                .withActions(S3Actions.AllS3Actions)
                .withResources(new Resource(arnId + ":s3:::" + BUCKET_NAME + path));
        statement.setId(sid);
        for (String accountId : accountIds) {
            statement.withPrincipals(new Principal(accountId));
        }
        return statement;
    }

}
