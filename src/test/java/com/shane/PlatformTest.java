package com.shane;

import org.junit.Test;

import java.util.ArrayList;

public class PlatformTest {

    final static Platform PLATFORM = new Platform();

    @Test
    public void testInitBucket() {
        boolean b = PLATFORM.initBucket();
        System.out.println(b);
    }

    @Test
    // TODO 测试共享用户是否可以修改 bucket 的 policy
    public void testUpdatePolicy() {
        ArrayList<String> users = new ArrayList<>();
        users.add("arn:aws-cn:iam::111222333444:user/test1");
        users.add("arn:aws-cn:iam::111222333444:user/test2");
        System.out.println(PLATFORM.updatePolicy(123123L, users));
    }

}
