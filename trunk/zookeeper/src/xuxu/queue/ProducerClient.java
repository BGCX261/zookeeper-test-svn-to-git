/**
 * Copyright &reg; 2004 Shanghai Tudou Co. Ltd.
 * All right reserved.
 */
package xuxu.queue;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * @author xxu
 * @date 2011-6-29
 * @version $id$
 */
public class ProducerClient {

    private ZooKeeper dataZooKeeper = null;
    private ZooKeeper indexZooKeeper = null;

    private String root;
    int sessionTimeout = 10000;

    public ProducerClient(String connectString, String root) {
        this.root = root;
        try {
            System.out.println("创建一个新的到根的连接:" + connectString);
            //dataZooKeeper = new ZooKeeper(connectString, sessionTimeout, this);
            dataZooKeeper = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
                // 监控所有被触发的事件
                public void process(WatchedEvent event) {
                    System.out.println("dataZooKeeper已经触发了" + event.getType() + "事件！");
                }
            });
        } catch (IOException e) {
            dataZooKeeper = null;
        }

        try {

            indexZooKeeper = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
                // 监控所有被触发的事件
                public void process(WatchedEvent event) {
                    System.out.println("indexZooKeeper已经触发了" + event.getType() + "事件！");
                }
            });

            Stat maxIndexStat = indexZooKeeper.exists("/index", false);
            if (maxIndexStat == null) {
                System.out.println("创建一个最大索引的节点: maxIndex");
                ByteBuffer b = ByteBuffer.allocate(4);
                b.putInt(0);
                indexZooKeeper.create("/index", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                indexZooKeeper.create("/index/maxIndex", b.array(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

                System.out.println("创建一个最小索引的节点: minIndex");
                ByteBuffer c = ByteBuffer.allocate(4);
                c.putInt(0);
                indexZooKeeper.create("/index/minIndex", c.array(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }

        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            Stat s = dataZooKeeper.exists(root, false);
            if (s == null) {
                dataZooKeeper.create(root, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (KeeperException e) {
            System.out.println(e);
        } catch (InterruptedException e) {
            System.out.println(e);
        }

    }

    public boolean produce(int i) throws KeeperException, InterruptedException {
        ByteBuffer b = ByteBuffer.allocate(4);
        byte[] value;
        b.putInt(i);
        value = b.array();
        int index = getIndex();
        String nodeName = "/element" + index;
        dataZooKeeper.create(root + nodeName, value, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        increaseIndex();
        System.out.println("Producer element" + index + " value : " + i);
        Thread.sleep(5 * 1000);
        return true;
    }

    public void increaseIndex() {
        try {
            byte[] value = indexZooKeeper.getData("/index/maxIndex", false, null);
            ByteBuffer buffer = ByteBuffer.wrap(value);
            int retvalue = buffer.getInt() + 1;
            ByteBuffer tmp = ByteBuffer.allocate(4);
            tmp.putInt(retvalue);
            indexZooKeeper.setData("/index/maxIndex", tmp.array(), -1);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void decreaseIndex() {
        try {
            byte[] value = indexZooKeeper.getData("/index/minIndex", false, null);
            ByteBuffer buffer = ByteBuffer.wrap(value);
            int retvalue = buffer.getInt() + 1;
            ByteBuffer tmp = ByteBuffer.allocate(4);
            tmp.putInt(retvalue);
            indexZooKeeper.setData("/index/minIndex", tmp.array(), -1);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public int getIndex() {
        byte[] value;
        try {
            value = indexZooKeeper.getData("/index/maxIndex", false, null);
            ByteBuffer buffer = ByteBuffer.wrap(value);
            int retvalue = buffer.getInt();
            return retvalue;
        } catch (KeeperException e) {
            e.printStackTrace();
            return 0;
        } catch (InterruptedException e) {
            e.printStackTrace();
            return 0;
        }

    }

    /**
     * @param args
     * @throws UnknownHostException
     */
    public static void main(String[] args) throws UnknownHostException {
        // TODO Auto-generated method stub

        String connectString = "10.5.16.32:2181";
        ProducerClient q = new ProducerClient(connectString, "/xuxu");
        String clientIp = InetAddress.getLocalHost().getHostAddress();

        System.out.println("Producer ++++++++++++++++++++++++++++++");
        for (int i = 1; i <= 5; i++) {
            try {
                DistributedLock lock = DistributedLockFactory.getLock("/lock", "10.5.16.32:2181");
                while (lock == null) {
                    Thread.sleep(500);
                    System.out.println("This is not your lock , please wait a moment!......");
                    lock = DistributedLockFactory.getLock("/lock", clientIp);
                }

                if (!lock.isLock()) {
                    lock.lock();
                    q.produce(i);
                    lock.unLock();
                }

            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (Exception e1) {
                e1.printStackTrace();
            }
        }

    }
}
