/**
 * Copyright &reg; 2004 Shanghai Tudou Co. Ltd.
 * All right reserved.
 */
package xuxu.test;

/**
 * @author xxu
 * @date 2011-6-27
 * @version $id$
 */
public class TestMain {

    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub

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

        Thread t1 = new ClientThread("10.5.16.32:2181", "server1", true);
        //Thread t2 = new ClientThread("10.5.16.105:2181", "server2", false);
        //Thread t3 = new ClientThread("10.5.16.32:2181", "server3", false);
        //Thread t4 = new ClientThread("10.5.16.105:2181", "server4", false);

        t1.start();
        //t2.start();
        //t3.start();
        // t4.start();
        //ControlThread c = new ControlThread(t1, t2, t3, t4);
        //c.start();
        int i = 0;
        while (true) {
            i++;
            i--;

        }

        /* 
         * 测试控制台输出： 
         * connectIP:server4,path:null,state:SyncConnected,type:None 
         * connectIP:server3,path:/test,state:SyncConnected,type:NodeChildrenChanged 
         * connectIP:server4,path:/test/server4,state:SyncConnected,type:NodeCreated 
         * 。。。。。。。。。。。 
         *  
         * connectIP:server2,path:null,state:Disconnected,type:None 
           server2exception,KeeperErrorCode = ConnectionLoss for /test 
           connectIP:newServer1,path:null,state:SyncConnected,type:None 
           connectIP:server1,path:/test,state:SyncConnected,type:NodeChildrenChanged 
           connectIP:server4,path:/test/server2,state:SyncConnected,type:NodeDeleted 
           connectIP:server4,path:/test,state:SyncConnected,type:NodeChildrenChanged 
           connectIP:newServer1,path:/test,state:SyncConnected,type:NodeChildrenChanged 
           connectIP:server3,path:/test/server2,state:SyncConnected,type:NodeDeleted 
           connectIP:server3,path:/test,state:SyncConnected,type:NodeChildrenChanged 
         */

    }

}
