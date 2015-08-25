/**
 * Copyright &reg; 2004 Shanghai Tudou Co. Ltd.
 * All right reserved.
 */
package xuxu.queue;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Collections;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooDefs.Perms;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

/**
 * @author xxu
 * @date 2011-6-30
 * @version $id$
 */
public class DistributedLockFactory {

    public static final ZooKeeper DEFAULT_ZOOKEEPER = getDefaultZookeeper();

    /*
     * 创建的锁有三种状态:
     * 1. 创建失败(null), 说明该锁被其他查询者使用了.’
     * 2. 创建成功, 但当前没有锁住(unlocked), 可以使用
     * 3. 创建成功, 但当前已经锁住(locked)了, 不能继续加锁.
     */
    //data格式:  ip:stat  如: 10.232.35.70:lock 10.232.35.70:unlock
    public static synchronized DistributedLock getLock(String path, String ip) throws Exception {
        if (DEFAULT_ZOOKEEPER != null) {
            Stat stat = null;
            try {
                stat = DEFAULT_ZOOKEEPER.exists(path, true);
            } catch (Exception e) {
                // TODO: use log system and throw new exception
            }
            if (stat != null) {
                byte[] data = DEFAULT_ZOOKEEPER.getData(path, null, stat);
                String dataStr = new String(data);
                String[] ipv = dataStr.split(":");
                if (ip.equals(ipv[0])) {
                    DistributedLock lock = new DistributedLock(path);
                    lock.setZooKeeper(DEFAULT_ZOOKEEPER);
                    return lock;
                } else {
                    //is not your lock, return null
                    return null;
                }
            } else {
                //创建成功, 但当前没有锁住(unlocked), 可以使用
                createZnode(path);
                DistributedLock lock = new DistributedLock(path);
                lock.setZooKeeper(DEFAULT_ZOOKEEPER);
                return lock;
            }
        }
        return null;
    }

    private static ZooKeeper getDefaultZookeeper() {
        try {
            ZooKeeper zooKeeper = new ZooKeeper("10.5.16.32:2181,10.5.16.104:2181,10.5.16.105:2181", 10 * 1000,
                    new Watcher() {
                        public void process(WatchedEvent event) {
                            //节点的事件处理. you can do something when the node's data change
                            //                  System.out.println("event " + event.getType() + " has happened!");
                        }
                    });
            return zooKeeper;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static void createZnode(String path) throws Exception {
        if (DEFAULT_ZOOKEEPER != null) {
            InetAddress address = InetAddress.getLocalHost();
            String data = address.getHostAddress() + ":unlock";
            DEFAULT_ZOOKEEPER.create(path, data.getBytes(),
                    Collections.singletonList(new ACL(Perms.ALL, Ids.ANYONE_ID_UNSAFE)), CreateMode.EPHEMERAL);
        }
    }

}
