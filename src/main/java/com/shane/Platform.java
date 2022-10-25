package com.shane;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.policy.Policy;
import com.amazonaws.auth.policy.PolicyReaderOptions;
import com.amazonaws.auth.policy.Principal;
import com.amazonaws.auth.policy.Resource;
import com.amazonaws.auth.policy.Statement;
import com.amazonaws.auth.policy.actions.S3Actions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.BucketPolicy;
import com.amazonaws.services.s3.model.BucketVersioningConfiguration;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.Region;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.SetBucketVersioningConfigurationRequest;
import com.amazonaws.services.s3.model.VersionListing;
import com.amazonaws.services.s3.model.ownership.ObjectOwnership;
import com.shane.enums.StoragePartitionEnum;
import lombok.extern.slf4j.Slf4j;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@Slf4j
public class Platform {

    // TODO 读取配置文件中的 ak / sk
    private String objectStoragePlatformAccessKey;
    private String objectStoragePlatformSecretKey;

    private ObjectStorage objectStorage = new ObjectStorage();

    private AmazonS3 s3;
    private String regionArn;

    // TODO 使用读取配置文件的参数替代此构造函数
    public Platform(ObjectStorage.Token token) {
        init(token);
    }

    private void init(ObjectStorage.Token token) {
        this.s3 = objectStorage.createClient(token);
        Region region = this.s3.getRegion();
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
        log.info(String.format("start init bucket [%s]", objectStorage.getBucketName()));
        log.info("check bucket exist...");
        if (doesBucketExist()) {
            log.info("bucket already exist");
            log.info("check bucket owner...");
            if (!isBucketOwner()) {
                log.error("check failed, unexpected owner");
                return false;
            }
            log.info("check passed, skip init");
        } else {
            log.info("bucket does not exist");
            log.info("> create bucket...");
            if (!createBucket()) {
                log.error("> create bucket failure");
                rollbackInitBucket(false);
                return false;
            }
            log.info("> create bucket success");
            log.info("> enable versioning...");
            if (!enableBucketVersioning()) {
                log.error("> enable versioning failure");
                rollbackInitBucket(true);
                return false;
            }
            log.info("> enable versioning success");
            log.info("init bucket success");
        }
        return true;
    }

    /**
     * <p>
     * 检测 bucket 的归属权，判断桶是否由当前账户创建
     * </p>
     *
     * @return 检测结果
     */
    private boolean isBucketOwner() {
        return s3.getS3AccountOwner().equals(getBucket().getOwner());
    }

    /**
     * <p>
     * 当 bucket 初始化失败时回滚
     * </p>
     *
     * @param bucketCreated 桶是否已经创建
     */
    private void rollbackInitBucket(boolean bucketCreated) {
        log.warn("init bucket failure");
        log.info("start rollback");
        if (bucketCreated) {
            log.info("> delete bucket...");
            if (!deleteBucket()) {
                log.error("> delete bucket failure");
                log.error("rollback failure");
                return;
            }
            log.info("> delete bucket success");
        }
        log.info("rollback success");
    }

    /**
     * <p>
     * 获取 workspace 的存储路径
     * </p>
     *
     * @param workspaceId workspace id
     * @return workspace 的存储路径
     */
    private String getWorkspacePath(Long workspaceId) {
        return "workspaces/" + workspaceId + "/";
    }

    public String getWorkspacePath(Long workspaceId, StoragePartitionEnum partition) {
        return getWorkspacePath(workspaceId) + partition.getPath();
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
        Policy policy = getBucketPolicy();
        Collection<Statement> statements = policy.getStatements();
        clearWorkspaceStatements(statements, workspaceId);
        if (userArns.size() != 0) {
            for (StoragePartitionEnum e : StoragePartitionEnum.values()) {
                if (e.isUserVisible()) {
                    statements.add(generateAllowAllActionsStatement(getWorkspacePath(workspaceId, e), userArns));
                }
            }
        }
        policy.setStatements(statements);
        return setBucketPolicy(fixAwsCnProblem(policy));
    }

    /**
     * <p>
     * 清除 bucket statements 中属于目标 workspace 的所有 statement
     * </p>
     *
     * @param statements  bucket 的 statements
     * @param workspaceId workspace id
     */
    private void clearWorkspaceStatements(Collection<Statement> statements, Long workspaceId) {
        ArrayList<String> needClearSids = new ArrayList<>();
        for (StoragePartitionEnum e : StoragePartitionEnum.values()) {
            if (e.isUserVisible()) {
                needClearSids.add(getAllowAllActionsStatementId(getWorkspacePath(workspaceId, e)));
            }
        }
        statements.removeIf(statement -> needClearSids.contains(statement.getId()));
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

    /**
     * <p>
     * 标准化路径格式，变为 a/b/c/
     * </p>
     *
     * @param path 路径
     * @return 标准化后的路径
     */
    private String normalizePath(String path) {
        if (path == null) {
            path = "";
        }
        if (!path.endsWith("/")) {
            path += "/";
        }
        if (path.startsWith("/")) {
            path = path.substring(1);
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
        return regionArn + ":s3:::" + objectStorage.getBucketName() + "/" + path + "*";
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

    ////////////////
    // S3 Actions //
    ////////////////

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
     * @return bucket，没有的话返回 null
     */
    private Bucket getBucket() {
        Bucket bucket = null;
        List<Bucket> buckets = listBuckets();
        for (Bucket b : buckets) {
            if (b.getName().equals(objectStorage.getBucketName())) {
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
     * @return true：存在 / false：不存在
     */
    private boolean doesBucketExist() {
        return s3.doesBucketExistV2(objectStorage.getBucketName());
    }

    /**
     * <p>
     * 创建桶
     * </p>
     *
     * @return 操作结果
     */
    private boolean createBucket() {
        try {
            CreateBucketRequest request = new CreateBucketRequest(objectStorage.getBucketName())
                    .withCannedAcl(CannedAccessControlList.Private)
                    .withObjectOwnership(ObjectOwnership.BucketOwnerEnforced);
            s3.createBucket(request);
        } catch (AmazonServiceException e) {
            log.error("[ Platform.createBucket # AmazonServiceException ]: " + e);
            return false;
        }
        return true;
    }

    /**
     * <p>
     * 删除桶
     * </p>
     *
     * @return 操作结果
     */
    private boolean deleteBucket() {
        try {
            s3.deleteBucket(objectStorage.getBucketName());
        } catch (AmazonServiceException e) {
            log.error("[ Platform.deleteBucket # AmazonServiceException ]: " + e);
            return false;
        }
        return true;
    }

    /**
     * <p>
     * 开启桶的版本控制
     * </p>
     *
     * @return 操作结果
     */
    private boolean enableBucketVersioning() {
        try {
            BucketVersioningConfiguration configuration = new BucketVersioningConfiguration(BucketVersioningConfiguration.ENABLED);
            SetBucketVersioningConfigurationRequest request = new SetBucketVersioningConfigurationRequest(objectStorage.getBucketName(), configuration);
            s3.setBucketVersioningConfiguration(request);
        } catch (AmazonServiceException e) {
            log.error("[ Platform.enableBucketVersioning # AmazonServiceException ]: " + e);
            return false;
        }
        return true;
    }

    /**
     * <p>
     * 设置桶策略
     * </p>
     *
     * @param policy 桶的策略
     * @return 操作结果
     */
    private boolean setBucketPolicy(Policy policy) {
        try {
            s3.setBucketPolicy(objectStorage.getBucketName(), policy.toJson());
        } catch (AmazonServiceException e) {
            log.error("[ Platform.setBucketPolicy # AmazonServiceException ]: " + e);
            return false;
        }
        return true;
    }

    /**
     * <p>
     * 获取桶策略
     * </p>
     *
     * @return 桶的策略
     */
    private Policy getBucketPolicy() {
        Policy policy = null;
        try {
            BucketPolicy bucketPolicy = s3.getBucketPolicy(objectStorage.getBucketName());
            String policyText = bucketPolicy.getPolicyText();
            policy = policyText == null ? new Policy() : Policy.fromJson(policyText, new PolicyReaderOptions().withStripAwsPrincipalIdHyphensEnabled(false));
        } catch (AmazonServiceException e) {
            log.error("[ Platform.getBucketPolicy # AmazonServiceException ]: " + e);
        }
        return policy;
    }

    /**
     * <p>
     * 包含子路径
     * </p>
     */
    public VersionListing listVersionsContainsSubDirectory(String prefix) {
        return s3.listVersions(objectStorage.getBucketName(), prefix);
    }

    /**
     * <p>
     * 复制文件夹下的所有内容到指定文件夹下，复制的内容为对象的最后一个版本
     * </p>
     * <p>
     * !!! 注意，目标路径应当是一个空路径，因为复制失败的时候会清空目标路径下的所有文件
     * </p>
     */
    public boolean copyDirectory(String sourcePath, String destinationPath) {
        VersionListing versionListing = listVersionsContainsSubDirectory(sourcePath);
        for (S3VersionSummary version : versionListing.getVersionSummaries()) {
            if (version.isLatest() && !version.isDeleteMarker()) {
                if (!copyVersion(version.getKey(), version.getVersionId(), version.getKey().replaceFirst(sourcePath, destinationPath))) {
                    deleteDirectory(destinationPath);
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * <p>
     * 复制对象的指定版本到指定路径，相当远上传一个对象到指定路径，所以重复复制会产生新的版本号
     * </p>
     * <p>
     * 仅供平台使用，用户行为不能调用此接口
     * </p>
     */
    public boolean copyVersion(String sourceKey, String sourceVersionId, String destinationKey) {
        try {
            s3.copyObject(new CopyObjectRequest(
                    objectStorage.getBucketName(), sourceKey, sourceVersionId,
                    objectStorage.getBucketName(), destinationKey));
            return true;
        } catch (AmazonServiceException e) {
            log.error("[ Platform.copyVersion # AmazonServiceException ]: " + e);
            return false;
        }
    }

    /**
     * <p>
     * 物理删除路径下的所有文件
     * </p>
     * <p>
     * 仅供平台使用，用户行为不能调用此接口
     * </p>
     */
    public boolean deleteDirectory(String path) {
        VersionListing versionListing = listVersionsContainsSubDirectory(path);
        for (S3VersionSummary version : versionListing.getVersionSummaries()) {
            if (!deleteVersion(version.getKey(), version.getVersionId())) {
                return false;
            }
        }
        return true;
    }

    /**
     * <p>
     * 物理删除
     * </p>
     */
    private boolean deleteVersion(String key, String versionId) {
        try {
            s3.deleteVersion(objectStorage.getBucketName(), key, versionId);
        } catch (AmazonServiceException e) {
            log.error("[ Platform.getBucketPolicy # AmazonServiceException ]: " + e);
            return false;
        }
        return true;
    }

    /**
     * <p>
     * 上传文件
     * </p>
     * <p>
     * 仅供平台使用，用户行为不能调用此接口
     * </p>
     */
    public boolean putObject(String key, InputStream inputStream) {
        try {
            s3.putObject(objectStorage.getBucketName(), key, inputStream, null);
        } catch (AmazonServiceException e) {
            log.error("[ Platform.putObject # AmazonServiceException ]: " + e);
            return false;
        }
        return true;
    }

}
