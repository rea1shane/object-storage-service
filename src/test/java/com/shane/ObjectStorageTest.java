package com.shane;

import com.shane.model.CommonSummary;
import org.junit.Test;

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
    public void testListVersions() {
        List<CommonSummary> commonSummaries = USER.listVersions(TOKEN, WORKSPACE_ID, "atlas-lib/");
        System.out.println(commonSummaries);
    }

}
