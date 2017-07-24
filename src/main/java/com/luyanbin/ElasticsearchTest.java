package com.luyanbin;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by luyb on 2017/7/3.
 */
public class ElasticsearchTest {

    private static TransportClient client;

    /**
     * ��־��
     */
    private static Logger logger = LogManager.getLogger(ElasticsearchTest.class);

    static {
        Settings esSettings = Settings.builder()
                .put("cluster.name", "my-application") //����ESʵ��������
                .put("client.transport.sniff", true) //�Զ���̽������Ⱥ��״̬���Ѽ�Ⱥ������ES�ڵ��ip��ӵ����صĿͻ����б���
                .build();
        client = new PreBuiltTransportClient(esSettings);//��ʼ��client���ϰ汾�����˱仯���˷����м������ط�������ʼ������ȡ�
        //�˲������IP������һ������ʵһ���͹��ˣ���Ϊ������Զ���̽����
        try {
            client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.240.5.201"), 9300));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    /**
     * �½�����
     * @param index ������
     */
    public void createNewIndex(String index){
        try{
            if(!isExistsIndex(index)){
                client.admin().indices().prepareCreate(index).get();
            }else{
                logger.info("������" + index + "�Ѿ����ڲ���Ҫ������");
            }
        }catch (Exception e){
            e.printStackTrace();
        }
//        client.admin().indices().prepareCreate(indexName).setSettings(settings).get();
    }

    /**
     * �ж�ָ�����������Ƿ����
     * @param indexName ������
     * @return  ���ڣ�true; �����ڣ�false;
     */
    public boolean isExistsIndex(String indexName){
        IndicesExistsResponse response =
                client.admin().indices().exists(
                        new IndicesExistsRequest().indices(new String[]{indexName})).actionGet();
        return response.isExists();
    }

    public static void main(String[] args) {

        ElasticsearchTest elasticsearchTest = new ElasticsearchTest();
        String index = "20170706";
        elasticsearchTest.createNewIndex(index);
        try {
//            addIndex(index,"test_lyb","101");
            for(int i=0;i<100;i++){
                addIndex(index,"test_lyb_del3",i+"");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            client.close();
        }
    }

    /**
     * ԭʼAPI ����client��������index��Ȼ�����type������id�����������в���
     * @param index ���� ���ݿ�
     * @param type ���� ��
     * @param id ���� ��
     * @throws IOException
     */
    private static void addIndex(String index, String type, String id) throws IOException {
        IndexResponse response = client.prepareIndex(index, type, id)
                .setSource(jsonBuilder()
                        .startObject()
                        .field("user", "lyb")
                        .field("postDate", new Date())
                        .field("message", "trying out Elasticsearch")
                        .endObject()
                )
                .get();
        //IndexResponse�����в����ķ��ؽ��
        logger.info(response.getResult());
    }
}
