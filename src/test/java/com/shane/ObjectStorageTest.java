package com.shane;

import com.shane.model.CommonSummary;
import com.shane.model.VersionSummaryVO;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
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
    // TODO 测试共享用户是否可以修改 bucket 的 policy
    public void testUpdatePolicy() {
        ArrayList<String> users = new ArrayList<>();
        // TODO 提交代码时清除这两项数据
        users.add("");
        users.add("");
        System.out.println(PLATFORM.updatePolicy(WORKSPACE_ID, users));
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
    public void testPutObject() throws FileNotFoundException {
        System.out.println(USER.putObject(TOKEN, "1.pdf", new FileInputStream("/Users/shane/Downloads/2022-01-20-S3权限控制.pdf")));
    }

    @Test
    public void testDeleteObject() {
        System.out.println(USER.deleteObject(TOKEN, "atlas-lib/.DS_Store"));
    }

}
