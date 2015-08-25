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
public class ControlThread extends Thread {

    public ControlThread(Thread t1, Thread t2, Thread t3, Thread t4) {
        list[0] = t1;
        list[1] = t2;
        list[2] = t3;
        list[3] = t4;
    }

    public ControlThread(Thread t1, Thread t2) {
        list[0] = t1;
        list[1] = t2;
    }

    private Thread[] list = new Thread[4];
    private int num = 0;

    public void run() {
        while (true) {
            if (num == 7) {
                list[2].stop();
                System.out.println("kill server3");
            }
            if (num == 15) {
                list[3].stop();
                System.out.println("kill server4");
            }
            try {
                sleep(1000);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block  
                e.printStackTrace();
            }
        }
    }

}
