/**
 * Copyright &reg; 2004 Shanghai Tudou Co. Ltd.
 * All right reserved.
 */
package xuxu;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

/**
 * @author xxu
 * @date 2011-7-8
 * @version $id$
 */
public class WatcherTest {

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        // 创建一个与服务器的连接
        ZooKeeper zk = new ZooKeeper("10.5.16.32:2181", 10000, new Watcher() {
            // 监控所有被触发的事件
            public void process(WatchedEvent event) {
                System.out.println("已经触发了" + event.getType() + "事件！");
            }
        });

        // 1.创建一个目录节点，并设置监听事件（只能用一次）
        zk.exists("/testRootPath", new Watcher() {
            // 监控所有被触发的事件
            public void process(WatchedEvent event) {
                System.out.println("已经触发了" + event.getType() + "事件:" + event.getPath());
            }
        });
        //2.这里会把NODECREATED事件监听到，但是之后再有别的事件，就监听不到了
        zk.create("/testRootPath", "testRootData".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        // 创建一个子目录节点
        //        zk.create("/testRootPath/testChildPathOne", "testChildDataOne".getBytes(), Ids.OPEN_ACL_UNSAFE,
        //                CreateMode.PERSISTENT);
        //        zk.create("/testRootPath/testChildPathTwo", "testChildDataOne".getBytes(), Ids.OPEN_ACL_UNSAFE,
        //                CreateMode.PERSISTENT);
        //System.out.println(new String(zk.getData("/testRootPath", false, null)));
        // 取出子目录节点列表
        //System.out.println(zk.getChildren("/testRootPath", true));
        // 修改子目录节点数据
        //zk.setData("/testRootPath/testChildPathOne", "modifyChildDataOne".getBytes(), -1);
        //System.out.println("目录节点状态：[" + zk.exists("/testRootPath", true) + "]");
        // 创建另外一个子目录节点
        //zk.create("/testRootPath/testChildPathTwo", "testChildDataTwo".getBytes(), Ids.OPEN_ACL_UNSAFE,
        //       CreateMode.PERSISTENT);
        //System.out.println(new String(zk.getData("/testRootPath/testChildPathTwo", true, null)));
        // 删除子目录节点
        // zk.delete("/testRootPath/testChildPathTwo", -1);
        //zk.delete("/testRootPath/testChildPathOne", -1);
        // 删除父目录节点
        zk.exists("/testRootPath", new Watcher() {
            // 监控所有被触发的事件
            public void process(WatchedEvent event) {
                System.out.println("已经触发了" + event.getType() + "事件:" + event.getPath());
            }
        });
        zk.delete("/testRootPath", -1);
        /*
         * 输出：
         *已经触发了None事件！
         *已经触发了NodeCreated事件:/testRootPath
         *已经触发了NodeDataChanged事件:/testRootPath
         *已经触发了NodeDeleted事件:/testRootPath
         */
        // 关闭连接
        zk.close();

    }

}
