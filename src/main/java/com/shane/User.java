package com.shane;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListVersionsRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.VersionListing;
import com.shane.model.CommonSummary;
import com.shane.model.DirectorySummaryVO;
import com.shane.model.VersionSummaryVO;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.impl.io.EmptyInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
// TODO 类中添加 user service 直接读取对应的 ak / sk，传递用户 token 变为传递 user id
// TODO copy object
// TODO move object (rename object)
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
        versions.removeIf(version -> !version.isLatest() || version.isDeleteMarker());
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

    public boolean mkdir(ObjectStorage.Token token, String path) {
        return putObject(createClient(token), path, EmptyInputStream.INSTANCE);
    }

    public boolean putObject(ObjectStorage.Token token, String key, InputStream inputStream) {
        return putObject(createClient(token), key, inputStream);
    }

    public boolean downloadVersion(ObjectStorage.Token token, String key, String versionId, OutputStream outputStream) {
        S3Object version = getVersion(createClient(token), key, versionId);
        if (version == null) {
            return false;
        }
        S3ObjectInputStream inputStream = version.getObjectContent();
        byte[] readBuf = new byte[1024];
        int readLen;
        try {
            while ((readLen = inputStream.read(readBuf)) > 0) {
                outputStream.write(readBuf, 0, readLen);
            }
            inputStream.close();
            outputStream.close();
        } catch (IOException e) {
            log.error("[ User.downloadVersion # IOException ]: " + e);
            return false;
        }
        return true;
    }

    public boolean deleteDirectory(ObjectStorage.Token token, String path) {
        return deleteDirectory(createClient(token), path);
    }

    public boolean deleteObjects(ObjectStorage.Token token, List<String> keys) {
        return deleteObjects(createClient(token), keys);
    }

    // TODO 做一个 exception handler，不同的 exception 返回不同的响应码

    ////////////////
    // S3 Actions //
    ////////////////

    /**
     * <p>
     * 没有做分页，可以用 maxResults 做分页，需要配合 listNextBatchOfVersions 方法
     * </p>
     */
    private VersionListing listVersions(AmazonS3 s3, String prefix) throws AmazonServiceException {
        return s3.listVersions(new ListVersionsRequest(
                objectStorage.getBucketName(), prefix, null, null, "/", null
        ));
    }

    private S3Object getVersion(AmazonS3 s3, String key, String versionId) {
        S3Object version = null;
        try {
            version = s3.getObject(new GetObjectRequest(objectStorage.getBucketName(), key, versionId));
        } catch (AmazonServiceException e) {
            log.error("[ User.getVersion # AmazonServiceException ]: " + e);
        }
        return version;
    }

    /**
     * <p>
     * 上传文件，不带有 metadata，也没有接受返回值
     * </p>
     */
    private boolean putObject(AmazonS3 s3, String key, InputStream inputStream) {
        try {
            s3.putObject(objectStorage.getBucketName(), key, inputStream, null);
        } catch (AmazonServiceException e) {
            log.error("[ User.putObject # AmazonServiceException ]: " + e);
            return false;
        }
        return true;
    }

    private boolean deleteDirectory(AmazonS3 s3, String path) {
        List<S3ObjectSummary> objectSummaries = s3.listObjects(objectStorage.getBucketName(), path).getObjectSummaries();
        List<String> deleteKeys = objectSummaries.stream().map(S3ObjectSummary::getKey).collect(Collectors.toList());
        return deleteObjects(s3, deleteKeys);
    }

    /**
     * <p>
     * 开启了版本控制后，此删除为逻辑删除
     * </p>
     * <p>
     * 批量删除
     * </p>
     */
    private boolean deleteObjects(AmazonS3 s3, List<String> keys) {
        try {
            keys.forEach(key -> s3.deleteObject(objectStorage.getBucketName(), key));
        } catch (AmazonServiceException e) {
            log.error("[ User.deleteObjects # AmazonServiceException ]: " + e);
            return false;
        }
        return true;
    }

}
