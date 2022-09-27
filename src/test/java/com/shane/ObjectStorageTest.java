package com.shane;

import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.VersionListing;
import com.shane.model.CommonSummary;
import com.shane.model.VersionSummaryVO;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;

public class ObjectStorageTest {

    // TODO 提交代码时清除这两项数据
    final static String ACCESS_KEY = "";
    final static String SECRET_KEY = "";
    final static ObjectStorage.Token TOKEN = ObjectStorage.Token.builder()
            .accessKey(ACCESS_KEY)
            .secretKey(SECRET_KEY)
            .build();

    final static Platform PLATFORM = new Platform(TOKEN);
    final static User USER = new User();
    final static Long WORKSPACE_ID = 123123L;

    @Test
    public void testInitBucket() {
        boolean b = PLATFORM.initBucket();
        System.out.println(b);
    }

    @Test
    public void testUpdatePolicy() {
        ArrayList<String> users = new ArrayList<>();
        // TODO 提交代码时清除这两项数据
        users.add("");
        users.add("");
        System.out.println(PLATFORM.updatePolicy(WORKSPACE_ID, users));
    }

    @Test
    public void testListVersionsContainsSubDirectory() {
        VersionListing versionListing = PLATFORM.listVersionsContainsSubDirectory("atlas-lib/");
        for (S3VersionSummary version : versionListing.getVersionSummaries()) {
            System.out.println(version.getKey() + " : " + version.getVersionId());
        }
    }

    @Test
    public void testCopyDirectory() {
        System.out.println(PLATFORM.copyDirectory("atlas-lib/", "test-copy/"));
    }

    @Test
    public void testCopyVersion() {
        System.out.println(PLATFORM.copyVersion("1.pdf", "FppxiA7IuKAZr_Z3T7iIRcy3sNPL9kDJ", "4.pdf"));
    }

    @Test
    public void testDeleteDirectoryPlatform() {
        System.out.println(PLATFORM.deleteDirectory("test-copy/"));
    }

    @Test
    public void testPutObjectPlatform() throws FileNotFoundException {
        System.out.println(PLATFORM.putObject("1.pdf", new FileInputStream("/Users/shane/Downloads/2022-01-20-S3权限控制.pdf")));
    }

    @Test
    public void testListLatestVersions() {
        List<CommonSummary> commonSummaries = USER.listLatestVersions(TOKEN, "atlas-lib/");
        System.out.println(commonSummaries);
    }

    @Test
    public void testListObjectVersions() {
        List<VersionSummaryVO> versions = USER.listObjectVersions(TOKEN, "atlas-lib/.DS_Store");
        System.out.println(versions);
    }

    @Test
    public void testMkdir() {
        System.out.println(USER.mkdir(TOKEN, "qqqqqq/aaaaa/ccccc/"));
    }

    @Test
    public void testPutObject() throws FileNotFoundException {
        System.out.println(USER.putObject(TOKEN, "1.pdf", new FileInputStream("/Users/shane/Downloads/2022-01-20-S3权限控制.pdf")));
    }

    @Test
    public void testDownloadVersion() throws FileNotFoundException {
        System.out.println(USER.downloadVersion(TOKEN, "1.pdf", "FppxiA7IuKAZr_Z3T7iIRcy3sNPL9kDJ", new FileOutputStream("2.pdf")));
    }

    @Test
    public void testDeleteDirectory() {
        System.out.println(USER.deleteDirectory(TOKEN, "atlas-lib/backup/"));
    }

    @Test
    public void testDeleteObjects() {
        ArrayList<String> deleteKeys = new ArrayList<>();
        deleteKeys.add("atlas-lib/.DS_Store");
        System.out.println(USER.deleteObjects(TOKEN, deleteKeys));
    }

}
