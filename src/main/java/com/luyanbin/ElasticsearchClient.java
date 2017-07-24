package com.luyanbin;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by luyb on 2017/7/6.
 */
public class ElasticsearchClient {

    private static TransportClient elasticsearchClient;

    private ElasticsearchClient(){

    }

    public static TransportClient getElasticsearchClient(){
        if(elasticsearchClient == null){
            elasticsearchClient = createClient();
        }
        return elasticsearchClient;
    }

    private static TransportClient createClient(){
        Settings esSettings = Settings.builder()
                .put("cluster.name", "my-application") //设置ES实例的名称
                .put("client.transport.sniff", true) //自动嗅探整个集群的状态，把集群中其他ES节点的ip添加到本地的客户端列表中
                .build();
        TransportClient client = new PreBuiltTransportClient(esSettings);//初始化client较老版本发生了变化，此方法有几个重载方法，初始化插件等。
        //此步骤添加IP，至少一个，其实一个就够了，因为添加了自动嗅探配置
        try {
            client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.240.5.201"), 9300));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return client;
    }
}
