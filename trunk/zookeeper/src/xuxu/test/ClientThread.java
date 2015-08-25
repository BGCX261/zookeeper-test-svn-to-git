/**
 * Copyright &reg; 2004 Shanghai Tudou Co. Ltd.
 * All right reserved.
 */
package xuxu.test;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.zookeeper.KeeperException;

import xuxu.test.operation.ServerOperation;
import xuxu.test.operation.ServerOperationImpl;

/**
 * @author xxu
 * @date 2011-6-27
 * @version $id$
 */
public class ClientThread extends Thread {

    public ClientThread(String address, String serverName, boolean islog) {
        this.address = address;
        this.serverName = serverName;
        try {
            otherOperation();
        } catch (KeeperException e) {
            // TODO Auto-generated catch block  
            e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block  
            e.printStackTrace();
        }
        this.islog = islog;
    }

    private boolean islog = true;
    private final String rootPath = "/test";
    private String address;
    private String serverName;
    private ServerOperation operationCient = null;

    public void run() {
        if (operationCient == null) {
            System.out.println("operationCient=null");
            return;
        }

        while (true) {
            try {
                if (islog) {
                    System.out.println(serverName + ",loopTime:" + getNowTime());
                }
                observerChildData(rootPath);
            } catch (KeeperException e) {
                // TODO Auto-generated catch block  
                System.out.println(serverName + "exception," + e.getLocalizedMessage());
                try {
                    operationCient = new ServerOperationImpl();
                    operationCient.init("10.5.16.104:2181", "newServer1");
                } catch (IOException e1) {
                    // TODO Auto-generated catch block  
                    System.out.println(serverName + " reconnect  exception," + e.getLocalizedMessage());
                }
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block  
                e.printStackTrace();
            }
            try {
                sleep(2000);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block  
                e.printStackTrace();
            }
        }

    }

    /* 
     * 测试流程 
     * 1、创建sever1的连接client1，并且创建一个永久性的/test节点 
     * 2、创建一个针对server1的临时节点 
     * 3、创建server2的连接client21，并创建一个针对server2的临时节点 
     * 4、创建server3的连接client3，并创建一个针对server3的临时节点 
     * 5、分别查看client1、client2、client3的三个节点的字节点数量，确定是否同步成功 
     * 6、修改client1的临时节点内容，然后在在client2和client3中查看 
     * 7、kill掉client3的线程，然后检查是watcher是否有通知给client1和client2 
     */

    private void otherOperation() throws KeeperException, InterruptedException {
        operationCient = new ServerOperationImpl();
        try {
            operationCient.init(address, serverName);
        } catch (IOException e) {
            // TODO Auto-generated catch block  
            e.printStackTrace();
        }

        if (operationCient == null) {
            System.out.println("operationCient=null");
            return;
        }
        if (!operationCient.exist(rootPath)) {
            operationCient.apendPresistentNode(rootPath, "this presistent-node is creat by " + serverName);
        }

        //添加临时节点  
        if (!operationCient.exist(rootPath + "/" + serverName)) {
            operationCient.apendTempNode(rootPath + "/" + serverName, "this temp-node is creat by " + serverName);
        }
        observerChildData("/test");

        //修改临时节点内容  
        operationCient.changeData(rootPath + "/" + serverName, "this temp-node is changed by " + serverName);
        //临时节点内容  
        List<String> childs = operationCient.getChilds(rootPath);
        for (String str : childs) {
            System.out.println("observered by " + serverName + ": child node is :" + str);
        }

    }

    //查看临时节点的同步状态  
    public void observerChildData(String path) throws KeeperException, InterruptedException {
        if (operationCient == null) {
            System.out.println("operationCient=null");
            return;
        }

        List<String> childs = operationCient.getChilds(rootPath);
        if (islog) {
            System.out.println("observered by " + serverName + ": childs len is :" + childs.size());
        }
        for (String str : childs) {
            if (islog) {
                System.out.println("observered by " + serverName + ": child node is :" + str + ",data is :"
                        + operationCient.getData(rootPath + "/" + str));
            }
        }

    }

    public String getNowTime() {
        DateFormat format1 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        return format1.format(new Date());
    }

}
