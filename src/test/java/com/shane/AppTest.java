package com.shane;

import org.junit.Test;

import java.util.ArrayList;

public class AppTest {

    final static App APP = new App();

    @Test
    public void testInitBucket() {
        boolean b = APP.initBucket();
        System.out.println(b);
    }

    @Test
    public void testUpdatePolicy() {
        ArrayList<String> users = new ArrayList<>();
        users.add("arn:aws-cn:iam::111222333444:user/test1");
        users.add("arn:aws-cn:iam::111222333444:user/test2");
        System.out.println(APP.updatePolicy(123123L, users));
    }

}
