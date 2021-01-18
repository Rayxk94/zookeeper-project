package com.xk.bigdata.curator.utils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class CuratorUtilsTest {

    final public String zkQuorum = "bigdatatest01:2181";
    final public int baseSleepTimeMs = 1000;
    final public int maxRetries = 5;
    final public String namespace = "spark";
    final public int sessionTimeoutMs = 1000;
    final public String path = "/hadoop";
    final public String value = "222";

    @Before
    public void setUp() {
        CuratorUtils.connect(zkQuorum, baseSleepTimeMs, maxRetries, namespace, sessionTimeoutMs);
    }

    @After
    public void cleanUp() {
        CuratorUtils.close();
    }

    /**
     * [zk: localhost:2181(CONNECTED) 6] ls /
     * [zookeeper, spark]
     * [zk: localhost:2181(CONNECTED) 7] ls /spark
     * [hadoop]
     * [zk: loca
     */
    @Test
    public void testCreateNode() {
        try {
            String result = CuratorUtils.createNode(path, value);
            System.out.println(result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGetData() {
        try {
            String result = CuratorUtils.getData(path);
            System.out.println(result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testSetData() {
        try {
            Boolean result = CuratorUtils.setData(path, "111");
            System.out.println(result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testCheckExists() {
        try {
            Boolean result = CuratorUtils.checkExists(path);
            System.out.println(result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testSetChildren() {
        for (int i = 0; i < 10; i++) {
            try {
                CuratorUtils.createNode(path + "/" + i, i + "");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testGetChildren() {
        try {
            List<String> childrens = CuratorUtils.getChildren(path);
            for (String children : childrens) {
                System.out.println(children);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testDelete() {
        try {
            CuratorUtils.delete(path);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
