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
     * 日志类
     */
    private static Logger logger = LogManager.getLogger(ElasticsearchTest.class);

    static {
        Settings esSettings = Settings.builder()
                .put("cluster.name", "my-application") //设置ES实例的名称
                .put("client.transport.sniff", true) //自动嗅探整个集群的状态，把集群中其他ES节点的ip添加到本地的客户端列表中
                .build();
        client = new PreBuiltTransportClient(esSettings);//初始化client较老版本发生了变化，此方法有几个重载方法，初始化插件等。
        //此步骤添加IP，至少一个，其实一个就够了，因为添加了自动嗅探配置
        try {
            client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.240.5.201"), 9300));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    /**
     * 新建索引
     * @param index 索引名
     */
    public void createNewIndex(String index){
        try{
            if(!isExistsIndex(index)){
                client.admin().indices().prepareCreate(index).get();
            }else{
                logger.info("索引：" + index + "已经存在不需要创建了");
            }
        }catch (Exception e){
            e.printStackTrace();
        }
//        client.admin().indices().prepareCreate(indexName).setSettings(settings).get();
    }

    /**
     * 判断指定的索引名是否存在
     * @param indexName 索引名
     * @return  存在：true; 不存在：false;
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
     * 原始API 根据client或者索引index，然后根据type（表），id（主键）进行插入
     * @param index 索引 数据库
     * @param type 类型 表
     * @param id 主键 键
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
        //IndexResponse对象有操作的返回结果
        logger.info(response.getResult());
    }
}
