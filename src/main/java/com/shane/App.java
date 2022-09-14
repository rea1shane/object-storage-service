package com.shane;

import com.amazonaws.auth.policy.Policy;
import com.amazonaws.auth.policy.PolicyReaderOptions;
import com.amazonaws.auth.policy.Principal;
import com.amazonaws.auth.policy.Resource;
import com.amazonaws.auth.policy.Statement;
import com.amazonaws.auth.policy.actions.S3Actions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.Region;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class App {

    // TODO 通过读取配置文件或者传参
    // 存储桶命名规则 https://docs.amazonaws.cn/AmazonS3/latest/userguide/bucketnamingrules.html
    private static final String BUCKET_NAME = "aml-bucket";

    private ObjectStorageService oss;
    private String regionArn;

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
                regionArn = "arn:aws-cn";
                break;
            default:
                regionArn = "arn:aws";
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
     * 更新 policy，会首先删除目标 workspace 的所有 statement，然后重新插入该 workspace 的所有 statement
     * </p>
     *
     * @param workspaceId workspace id
     * @param userArns    用户 arn 列表
     * @return 操作结果
     */
    public boolean updatePolicy(Long workspaceId, List<String> userArns) {
        Policy policy = oss.getBucketPolicy(BUCKET_NAME);
        Collection<Statement> statements = policy.getStatements();
        cleanWorkspaceStatements(statements, workspaceId);
        if (userArns.size() != 0) {
            statements.add(generateAllowAllActionsStatement(getWorkspacePath(workspaceId), userArns));
        }
        policy.setStatements(statements);
        return oss.setBucketPolicy(BUCKET_NAME, fixAwsCnProblem(policy));
    }

    /**
     * <p>
     * 清除 bucket statements 中属于目标 workspace 的所有 statement
     * </p>
     *
     * @param statements  bucket 的 statements
     * @param workspaceId workspace id
     */
    private void cleanWorkspaceStatements(Collection<Statement> statements, Long workspaceId) {
        String allowAllSid = getAllowAllActionsStatementId(getWorkspacePath(workspaceId));
        statements.removeIf(statement -> statement.getId().equals(allowAllSid));
    }

    /**
     * <p>
     * 生成具有对桶路径全部操作权限的 statement
     * </p>
     *
     * @param path     资源路径
     * @param userArns 用户 arn 列表
     * @return statement
     */
    private Statement generateAllowAllActionsStatement(String path, List<String> userArns) {
        Statement statement = new Statement(Statement.Effect.Allow)
                .withResources(new Resource(getResourceArn(path)))
                .withActions(S3Actions.AllS3Actions);
        statement.setPrincipals(convertArnsToPrincipals(userArns));
        statement.setId(getAllowAllActionsStatementId(path));
        return statement;
    }

    private String getAllowAllActionsStatementId(String path) {
        return normalizePath(path) + "-AllowAllActions";
    }

    private String getWorkspacePath(Long workspaceId) {
        return "/workspaces/" + workspaceId;
    }

    /**
     * <p>
     * 标准化路径格式，变为 /a/b/c
     * </p>
     *
     * @param path 路径
     * @return 标准化后的路径
     */
    private String normalizePath(String path) {
        if (path == null) {
            path = "/";
        }
        if (!path.startsWith("/")) {
            path = "/" + path;
        }
        if (path.endsWith("/") && path.length() != 1) {
            path = path.substring(0, path.length() - 1);
        }
        return path;
    }

    /**
     * <p>
     * 生成资源的 arn
     * </p>
     *
     * @param path 资源路径
     * @return 资源 arn
     */
    private String getResourceArn(String path) {
        path = normalizePath(path);
        if (path.equals("/")) {
            path = "";
        }
        return regionArn + ":s3:::" + BUCKET_NAME + path + "/*";
    }

    /**
     * <p>
     * 将用户 arn 列表转换为  principal 列表
     * </p>
     *
     * @param userArns 用户 arn 列表
     * @return principal 列表
     */
    private List<Principal> convertArnsToPrincipals(List<String> userArns) {
        ArrayList<Principal> principals = new ArrayList<>();
        for (String arn : userArns) {
            principals.add(new Principal("AWS", arn, false));
        }
        return principals;
    }

    /**
     * <p>
     * 修复 policy 中的 principal 在创建时会自动将 aws-cn 转换为 awscn 的问题
     * </p>
     *
     * @param policy 要修复的 policy
     * @return 修复后的 policy
     */
    private Policy fixAwsCnProblem(Policy policy) {
        String policyJson = policy.toJson();
        String fixedJson = policyJson.replace("awscn", "aws-cn");
        return Policy.fromJson(fixedJson, new PolicyReaderOptions().withStripAwsPrincipalIdHyphensEnabled(false));
    }

}
