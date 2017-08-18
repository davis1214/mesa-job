package com.di.mesa.plugin.zookeeper;

import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.lang.time.DateFormatUtils;

import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.lang.System.out;

/**
 * Created by Davi on 17/8/16.
 */
public class ZkConfigSubscriber {

    public static void main(String[] args) throws InterruptedException {

        String zkAddress = "localhost:2181";
        int connectionTimeout = 30000;

        String baseRoot = "/mesa/conf";

        String subscribeFile = "mesa.properties";

        //TODO String zkServers, int connectionTimeout
        ZkClient client = new ZkClient(zkAddress, connectionTimeout);
        client.setZkSerializer(new ZkStringSerializer("UTF-8"));

        IZkConfigChangeSubscriber subscriber = new ZkConfigChangeSubscriber(client, baseRoot);


        while (!client.exists(baseRoot + "/" + subscribeFile)) {
            out.println(DateFormatUtils.format(new Date(), "yyyy-MM-dd HH:mm:SS") + " | 检查配置是否存在,当前状态 -> " + baseRoot + "/" + subscribeFile);
            Thread.sleep(5000l);
        }

        out.println("start to observe distribute configuration!!");

        final CountDownLatch countDownLatch = new CountDownLatch(10);
        subscriber.subscribe(subscribeFile, new IZkConfigChangeListener() {
            public void configChanged(String key, String value) {
                out.println("test1接收到数据变更通知: key=" + key + ", value=" + value);

                countDownLatch.countDown();
            }
        });

        countDownLatch.await(Long.MAX_VALUE, TimeUnit.SECONDS);
    }

}
