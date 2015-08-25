/**
 * Copyright &reg; 2004 Shanghai Tudou Co. Ltd.
 * All right reserved.
 */
package xuxu.test.operation;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.KeeperException;

/**
 * @author xxu
 * @date 2011-6-27
 * @version $id$
 */
public interface ServerOperation {

    void init(String address, String serverName) throws IOException;

    void destroy() throws InterruptedException;

    List<String> getChilds(String path) throws KeeperException, InterruptedException;

    String getData(String path) throws KeeperException, InterruptedException;

    void changeData(String path, String data) throws KeeperException, InterruptedException;

    void delData(String path) throws KeeperException, InterruptedException;

    void apendTempNode(String path, String data) throws KeeperException, InterruptedException;

    void apendPresistentNode(String path, String data) throws KeeperException, InterruptedException;

    void delNode(String path) throws KeeperException, InterruptedException;

    boolean exist(String path) throws KeeperException, InterruptedException;

}
