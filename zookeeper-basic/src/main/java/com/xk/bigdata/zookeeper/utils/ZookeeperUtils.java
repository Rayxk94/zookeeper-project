package com.xk.bigdata.zookeeper.utils;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.List;

public class ZookeeperUtils {

    public static ZooKeeper client = null;

    /**
     * 得到Zookeeper客户端
     *
     * @param connectString
     * @param sessionTimeout
     * @param watcher
     * @throws Exception
     */
    public static void getZookeeperClient(String connectString, int sessionTimeout, Watcher watcher) throws Exception {
        client = new ZooKeeper(connectString, sessionTimeout, watcher);
    }

    /**
     * 关闭Zookeeper客户端
     *
     * @throws Exception
     */
    public static void close() throws Exception {
        if (null != client) {
            client.close();
        }
    }

    /**
     * 创建节点
     *
     * @param path  路径
     * @param value 数据
     * @return
     * @throws Exception
     */
    public static String create(String path, String value) throws Exception {
        return client.create(path, value.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    /**
     * 判断节点是否存在
     *
     * @param path 路径
     * @return
     * @throws Exception
     */
    public static Boolean exists(String path) throws Exception {
        return client.exists(path, false) == null ? false : true;
    }

    /**
     * 得到该目录下面的数据
     *
     * @param path 路径
     * @param stat 监听
     * @return
     * @throws Exception
     */
    public static String getData(String path, Stat stat) throws Exception {
        return new String(client.getData(path, false, stat));
    }

    /**
     * 得到所有的子目录
     *
     * @param path
     * @return
     * @throws Exception
     */
    public static List<String> getChildren(String path) throws Exception {
        return client.getChildren(path, false);
    }

    /**
     * 修改路径数据
     *
     * @param path  路径
     * @param value 数据
     * @return
     * @throws Exception
     */
    public static Boolean setData(String path, String value) throws Exception {
        client.setData(path, value.getBytes(), -1);
        Stat stat = new Stat();
        String data = getData(path, stat);
        if (value.equals(data)) {
            return true;
        }
        return false;
    }

    /**
     * 删除节点
     *
     * @param path      路径
     * @param recursive 是否删除下面子节点
     * @throws Exception
     */
    public static void delete(String path, Boolean recursive) throws Exception {
        if (recursive) {
            deleteChildren(path);
            client.delete(path, -1);
        } else {
            client.delete(path, -1);
        }
    }

    /**
     * 删除子节点
     *
     * @param path 路径
     * @throws Exception
     */
    public static void deleteChildren(String path) throws Exception {
        List<String> childrens = getChildren(path);
        if (childrens.isEmpty()) {
            client.delete(path, -1);
        } else {
            for (String children : childrens) {
                deleteChildren(path + "/" + children);
            }
        }
    }

}
