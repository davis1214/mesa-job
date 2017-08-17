package com.di.mesa.plugin.zk;

import com.di.mesa.plugin.zookeeper.ZkStringSerializer;
import org.I0Itec.zkclient.ZkClient;

import static java.lang.System.out;

/**
 * Created by Davi on 17/8/17.
 */
public class ZookeeperClientTest {


    public static void main(String[] args) {

        ZkClient zkClient = new ZkClient("127.0.0.1:2181", 10000);
        zkClient.setZkSerializer(new ZkStringSerializer("UTF-8"));


        out.println("------>" + zkClient.exists("/yard"));
    }

}
