package com.xk.bigdata.zookeeper.utils;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class ZookeeperUtilsTest {

    public String connectString = "bigdatatest01:2181";

    public int sessionTimeout = 5000;

    public String path = "/hadoop";

    public String value = "111";

    @Before
    public void setUp() {
        try {
            ZookeeperUtils.getZookeeperClient(connectString, sessionTimeout, new Watcher() {
                @Override
                public void process(WatchedEvent watchedEvent) {

                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @After
    public void cleanUp() {
        try {
            ZookeeperUtils.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testCreate() {
        try {
            String result = ZookeeperUtils.create(path, value);
            System.out.println(result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testExists() {
        try {
            Boolean result = ZookeeperUtils.exists(path);
            System.out.println(result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGetData() {
        Stat stat = new Stat();
        try {
            String result = ZookeeperUtils.getData(path, stat);
            System.out.println(result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testCreateChildren() {
        for (int i = 0; i < 10; i++) {
            try {
                ZookeeperUtils.create(path + "/" + i, i + "");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testGetChildren() {
        try {
            List<String> childrens = ZookeeperUtils.getChildren(path);
            for (String children : childrens) {
                System.out.println(children);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testSetData() {
        try {
            Boolean result = ZookeeperUtils.setData(path, "2222");
            System.out.println(result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testDelete() {
        try {
            ZookeeperUtils.delete(path, true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
