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
                .put("cluster.name", "my-application") //����ESʵ��������
                .put("client.transport.sniff", true) //�Զ���̽������Ⱥ��״̬���Ѽ�Ⱥ������ES�ڵ��ip��ӵ����صĿͻ����б���
                .build();
        TransportClient client = new PreBuiltTransportClient(esSettings);//��ʼ��client���ϰ汾�����˱仯���˷����м������ط�������ʼ������ȡ�
        //�˲������IP������һ������ʵһ���͹��ˣ���Ϊ������Զ���̽����
        try {
            client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.240.5.201"), 9300));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return client;
    }
}
