package xuxu.queue;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class Client implements Watcher {
    protected static ZooKeeper dataZooKeeper = null;
    protected static ZooKeeper indexZooKeeper = null;

    int sessionTimeout = 10000;
    protected String root;
    private String connectString;

    public Client(String connectString) {
        this.connectString = connectString;

        if (dataZooKeeper == null) {

        }

        if (indexZooKeeper == null) {

        }
    }

    synchronized public void process(WatchedEvent event) {
        String outputStr = "";
        if (this.connectString != null) {
            outputStr += "connectIP: " + this.connectString;
        }
        outputStr += ",  eventPath:" + event.getPath();
        outputStr += ",  eventState:" + event.getState();
        outputStr += ",  eventType:" + event.getType();

        System.out.println(outputStr);
    }
}
