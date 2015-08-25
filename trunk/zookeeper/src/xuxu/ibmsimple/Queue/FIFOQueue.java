package xuxu.ibmsimple.Queue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import xuxu.ibmsimple.TestMainClient;

/**
 * FIFOQueue
 * <p/>
 * Author By: junshan Created Date: 2010-9-7 14:09:19
 */
public class FIFOQueue extends TestMainClient {
    public static final Logger logger = Logger.getLogger(FIFOQueue.class);

    /**
     * Constructor
     * 
     * @param connectString
     * @param root
     */
    FIFOQueue(String connectString, String root) {
        super(connectString);
        this.root = root;
        if (zk != null) {
            try {
                Stat s = zk.exists(root, false);
                if (s == null) {
                    zk.create(root, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }
            } catch (KeeperException e) {
                System.out.println(e);
            } catch (InterruptedException e) {
                System.out.println(e);
            }
        }
    }

    /**
     * 生产者
     * 
     * @param i
     * @return
     */

    boolean produce(int i) throws KeeperException, InterruptedException {
        ByteBuffer b = ByteBuffer.allocate(4);
        byte[] value;
        b.putInt(i);
        value = b.array();
        String nodeName = "/element" + i;
        zk.create(root + nodeName, value, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
        System.out.println("Producer element" + (i) + " value : " + i);
        Thread.sleep(8 * 1000);
        return true;
    }

    /**
     * 消费者
     * 
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    int consume() throws KeeperException, InterruptedException {
        int retvalue = -1;
        Stat stat = null;
        while (true) {
            synchronized (mutex) {
                List<String> list = zk.getChildren(root, true);
                if (list.size() == 0) {
                    mutex.wait();
                } else {
                    Integer min = new Integer(list.get(0).substring(7));
                    for (String s : list) {
                        Integer tempValue = new Integer(s.substring(7));
                        if (tempValue < min)
                            min = tempValue;
                    }
                    byte[] b = zk.getData(root + "/element" + min, false, stat);
                    zk.delete(root + "/element" + min, 0);
                    Thread.sleep(8 * 1000);
                    ByteBuffer buffer = ByteBuffer.wrap(b);
                    retvalue = buffer.getInt();
                    return retvalue;
                }
            }
        }
    }

    @Override
    public void process(WatchedEvent event) {
        super.process(event);
    }

    public static void main(String args[]) {
        //启动Server
        //TestMainServer.start();
        String connectString = "10.5.16.32:2181";

        FIFOQueue q = new FIFOQueue(connectString, "/xuxu");

        final String cs = connectString;

        final String lock = "lock";

        final String process = "process";

        new Thread(new Runnable() {
            public void run() {
                if (zk == null) {
                    System.out.println("zk=null");
                    return;
                }

                while (true) {

                    try {
                        synchronized (lock) {
                            lock.wait();
                        }
                    } catch (InterruptedException e2) {
                        e2.printStackTrace();
                    }

                    System.out.println(cs + " KeeperException");
                    try {
                        zk = new ZooKeeper("10.5.16.105:2181", 10000, new Watcher() {
                            public void process(WatchedEvent event) {
                                //synchronized (mutex) {
                                //mutex.notify();
                                //}
                                String outputStr = "connectServer: 10.5.16.105:2181";
                                outputStr += ",  eventPath:" + event.getPath();
                                outputStr += ",  eventState:" + event.getState();
                                outputStr += ",  eventType:" + event.getType();

                                System.out.println(outputStr);
                            }
                        });
                        synchronized (process) {
                            process.notifyAll();
                        }
                    } catch (IOException e1) {
                        // TODO Auto-generated catch block  
                        System.out.println(cs + " reconnect  exception," + e1.getLocalizedMessage());
                    }
                }
            }
        }).start();

        int i;
        Integer max = new Integer(5);

        System.out.println("Producer ++++++++++++++++++++++++++++++");
        for (i = 1; i <= max; i++) {
            try {
                //先传创建结点然后再重链接，所以总会丢失一个
                q.produce(i);
            } catch (KeeperException e) {
                System.out.println(e);
                synchronized (lock) {
                    lock.notifyAll();
                }
                synchronized (process) {
                    try {
                        System.out.println("Producer waiting connect.......");
                        process.wait();
                    } catch (InterruptedException e1) {
                        e1.printStackTrace();
                    }
                }
            } catch (InterruptedException e) {
                System.out.println(e);
            }
        }

        try {
            Thread.sleep(60 * 1000);
        } catch (InterruptedException e1) {
            e1.printStackTrace();
        }
        System.out.println("Consumer ++++++++++++++++++++++++++++++");
        for (i = 0; i < max; i++) {
            try {
                int r = q.consume();
                System.out.println("Element" + i + " value : " + r);
            } catch (KeeperException e) {
                synchronized (lock) {
                    lock.notifyAll();
                }

                synchronized (process) {
                    try {
                        process.wait();
                        System.out.println("Consumer waiting connect.......");
                    } catch (InterruptedException e1) {
                        e1.printStackTrace();
                    }
                }
                i--;
                System.out.println(e);
            } catch (InterruptedException e) {
                System.out.println(e);
            }
        }

    }
}
