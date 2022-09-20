package com.shane;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListVersionsRequest;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.VersionListing;
import com.shane.model.CommonSummary;
import com.shane.model.DirectorySummaryVO;
import com.shane.model.VersionSummaryVO;
import lombok.extern.slf4j.Slf4j;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

@Slf4j
// TODO 类中添加 user service 直接读取对应的 ak / sk，传递用户 token 变为传递 user id
public class User {

    // TODO 将这个类变为 service
    public User() {

    }

    // TODO 通过 Spring 注入
    private ObjectStorage objectStorage = new ObjectStorage();

    private AmazonS3 createClient(ObjectStorage.Token token) {
        return objectStorage.createClient(token);
    }

    public List<CommonSummary> listLatestVersions(ObjectStorage.Token token, String prefix) throws AmazonServiceException {
        VersionListing versionListing = listVersions(createClient(token), prefix);

        List<String> dirs = versionListing.getCommonPrefixes();
        List<DirectorySummaryVO> directorySummaryVOs = DirectorySummaryVO.generateDirectorySummaryVOList(dirs);
        List<CommonSummary> result = new ArrayList<>(directorySummaryVOs);

        List<S3VersionSummary> versions = versionListing.getVersionSummaries();
        versions.removeIf(version -> !version.isLatest());
        List<VersionSummaryVO> versionSummaryVOS = VersionSummaryVO.generateVersionSummaryVOList(versions);
        result.addAll(versionSummaryVOS);

        return result;
    }

    public List<VersionSummaryVO> listObjectVersions(ObjectStorage.Token token, String key) throws AmazonServiceException {
        AmazonS3 s3 = createClient(token);
        VersionListing versionListing = listVersions(s3, key);
        List<S3VersionSummary> versions = versionListing.getVersionSummaries();
        versions.removeIf(version -> !version.getKey().equals(key));
        return VersionSummaryVO.generateVersionSummaryVOList(versions);
    }

    public boolean putObject(ObjectStorage.Token token, String key, InputStream inputStream) {
        return putObject(createClient(token), key, inputStream);
    }

    public boolean deleteObject(ObjectStorage.Token token, String key) {
        return deleteObject(createClient(token), key);
    }

    // TODO 做一个 exception handler，不同的 exception 返回不同的响应码

    ////////////////
    // S3 Actions //
    ////////////////

    /**
     * <p>
     * 没有做分页，可以用 maxResults 做分页，需要配合 listNextBatch 方法
     * </p>
     */
    private VersionListing listVersions(AmazonS3 s3, String prefix) throws AmazonServiceException {
        return s3.listVersions(new ListVersionsRequest(
                objectStorage.getBucketName(), prefix, null, null, "/", null
        ));
    }

    /**
     * <p>
     * 上传文件，不带有 metadata，也没有接受返回值
     * </p>
     */
    private boolean putObject(AmazonS3 s3, String key, InputStream inputStream) {
        try {
            s3.putObject(objectStorage.getBucketName(), key, inputStream, null);
            return true;
        } catch (AmazonServiceException e) {
            log.error("[ User.putObject ]: " + e);
            return false;
        }
    }

    /**
     * <p>
     * 开启了版本控制后，此删除为逻辑删除
     * </p>
     */
    private boolean deleteObject(AmazonS3 s3, String key) {
        try {
            s3.deleteObject(objectStorage.getBucketName(), key);
            return true;
        } catch (AmazonServiceException e) {
            log.error("[ User.deleteObject ]: " + e);
            return false;
        }
    }

}
