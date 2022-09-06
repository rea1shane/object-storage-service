package com.shane;

import org.junit.Test;

public class AppTest {

    final static App APP = new App();

    @Test
    public void testInitBucket() {
        boolean b = APP.initBucket();
        System.out.println(b);
    }

}
