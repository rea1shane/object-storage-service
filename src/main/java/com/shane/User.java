package com.shane;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListVersionsRequest;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.VersionListing;
import com.shane.model.CommonSummary;
import com.shane.model.DirectorySummaryVO;
import com.shane.model.VersionSummaryVO;

import java.util.ArrayList;
import java.util.List;

public class User {

    // TODO 将这个类变为 service
    public User() {

    }

    // TODO 通过 Spring 注入
    private ObjectStorage objectStorage = new ObjectStorage();

    private AmazonS3 createClient(ObjectStorage.Token token) {
        return objectStorage.createClient(token);
    }

    public List<CommonSummary> listVersions(ObjectStorage.Token token, Long workspaceId, String prefix) {
        AmazonS3 s3 = createClient(token);
        VersionListing versionListing = listVersions(s3, objectStorage.getWorkspacePath(workspaceId) + prefix);

        List<String> dirs = versionListing.getCommonPrefixes();
        List<DirectorySummaryVO> directorySummaryVOs = DirectorySummaryVO.generateDirectorySummaryVOList(dirs);
        List<CommonSummary> result = new ArrayList<>(directorySummaryVOs);

        List<S3VersionSummary> versions = versionListing.getVersionSummaries();
        List<VersionSummaryVO> versionSummaryVOS = VersionSummaryVO.generateVersionSummaryVOList(versions);
        result.addAll(versionSummaryVOS);

        return result;
    }

    ////////////////
    // S3 Actions //
    ////////////////

    private VersionListing listVersions(AmazonS3 s3, String prefix) {
        return s3.listVersions(new ListVersionsRequest(
                objectStorage.getBucketName(), prefix, null, null, "/", null
        ));
    }

}
