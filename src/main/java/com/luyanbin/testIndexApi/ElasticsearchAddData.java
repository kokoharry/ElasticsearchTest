package com.luyanbin.testIndexApi;

import com.luyanbin.ElasticsearchClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexResponse;

import java.io.IOException;
import java.util.*;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by luyb on 2017/7/6.
 */
public class ElasticsearchAddData {

    /**
     * ��־��
     */
    private static Logger logger = LogManager.getLogger(ElasticsearchAddData.class);
    /**
     * ԭʼAPI ����client��������index��Ȼ�����type������id�����������в���
     * @param index ���� ���ݿ�
     * @param type ���� ��
     * @param id ���� ��
     * @throws IOException
     */
    private void addData(String index, String type, String id) throws IOException {
        IndexResponse response = ElasticsearchClient.getElasticsearchClient().prepareIndex(index, type, id)
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

    private void addDataByBulk(List<Map<String,String>> list) throws IOException {
        BulkRequestBuilder bulkRequest = ElasticsearchClient.getElasticsearchClient().prepareBulk();
// either use client#prepare, or use Requests# to directly build index/delete requests
        for(Map<String,String> map : list){
            bulkRequest.add(ElasticsearchClient.getElasticsearchClient().prepareIndex(map.get("index"),
                    map.get("type"), map.get("id"))
                    .setSource(jsonBuilder()
                            .startObject()
                            .field("user", "lyb")
                            .field("postDate", new Date())
                            .field("message", "trying out Elasticsearch")
                            .endObject()
                    )
            );
        }
        BulkResponse bulkResponse = bulkRequest.get();
        if (bulkResponse.hasFailures()) {
            // process failures by iterating through each bulk response item
            logger.error(bulkResponse.buildFailureMessage());
        }
    }

    public static void main(String[] args) {
        try{
            ElasticsearchAddData elasticsearchTest = new ElasticsearchAddData();
            String index = "20170703";
            String type = System.currentTimeMillis() +"";
            List<Map<String,String>> list = new ArrayList<>();
            for(int i=0;i<10;i++){
                elasticsearchTest.addData(index, type, i + "");
                Map<String,String> map = new HashMap<>();
                map.put("index",index);
                map.put("type",type + "bulk");
                map.put("id",i+"");
                list.add(map);
            }
            elasticsearchTest.addDataByBulk(list);
        }catch (Exception e){
            logger.error("",e);
        }finally {
            ElasticsearchClient.getElasticsearchClient().close();
        }
    }
}
