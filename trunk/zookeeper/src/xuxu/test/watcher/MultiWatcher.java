/**
 * Copyright &reg; 2004 Shanghai Tudou Co. Ltd.
 * All right reserved.
 */
package xuxu.test.watcher;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

/**
 * @author xxu
 * @date 2011-6-27
 * @version $id$
 */
public class MultiWatcher implements Watcher {

    public MultiWatcher(String address) {
        connectAddress = address;
    }

    private String connectAddress = null;

    @Override
    public void process(WatchedEvent event) {
        // TODO Auto-generated method stub  
        //if (event.getType() == Watcher.Event.EventType.NodeCreated) {
        String outputStr = "";
        if (connectAddress != null) {
            outputStr += "connectIP: " + connectAddress;
        }
        outputStr += ",  eventPath:" + event.getPath();
        outputStr += ",  eventState:" + event.getState();
        outputStr += ",  eventType:" + event.getType();

        System.out.println(outputStr);
        //}

    }

}
