package com.xk.bigdata.curator.utils;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;

import java.util.List;

public class CuratorUtils {

    public static CuratorFramework client = null;

    /**
     * 连接 Curator 客户端
     *
     * @param zkQuorum         Zookeeper连接字符串
     * @param baseSleepTimeMs  基本sleep时间
     * @param maxRetries       最大尝试次数
     * @param namespace        命名空间
     * @param sessionTimeoutMs Session 最大超时时间
     */
    public static void connect(String zkQuorum, int baseSleepTimeMs, int maxRetries, String namespace, int sessionTimeoutMs) {
        ExponentialBackoffRetry retry = new ExponentialBackoffRetry(baseSleepTimeMs, maxRetries);
        client = CuratorFrameworkFactory.builder()
                .connectString(zkQuorum)
                .sessionTimeoutMs(sessionTimeoutMs)
                .retryPolicy(retry)
                .namespace(namespace)
                .build();
        client.start();
    }

    /**
     * 关闭客户端
     */
    public static void close() {
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
    public static String createNode(String path, String value) throws Exception {
        return client.create().creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT)
                .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                .forPath(path, value.getBytes());
    }

    /**
     * 得到节点数据
     *
     * @param path 目录
     * @return
     * @throws Exception
     */
    public static String getData(String path) throws Exception {
        return new String(client.getData().forPath(path));
    }

    /**
     * 修改节点数据
     *
     * @param path  路径
     * @param value 数据
     * @return
     * @throws Exception
     */
    public static Boolean setData(String path, String value) throws Exception {
        client.setData()
                .forPath(path, value.getBytes());
        String data = getData(path);
        if (data.equals(value)) {
            return true;
        }
        return false;
    }

    /**
     * 检查节点是否存在
     *
     * @param path 路径
     * @return
     * @throws Exception
     */
    public static Boolean checkExists(String path) throws Exception {
        return client.checkExists()
                .forPath(path) == null ? false : true;
    }

    /**
     * 得到子节点
     *
     * @param path 路径
     * @return
     * @throws Exception
     */
    public static List<String> getChildren(String path) throws Exception {
        return client.getChildren()
                .forPath(path);
    }

    /**
     * 删除路径
     *
     * @param path 路径
     * @throws Exception
     */
    public static void delete(String path) throws Exception {
        client.delete()
                .deletingChildrenIfNeeded()
                .forPath(path);
    }

}
