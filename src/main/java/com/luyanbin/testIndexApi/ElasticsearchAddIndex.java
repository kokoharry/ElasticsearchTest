package com.luyanbin.testIndexApi;

import com.luyanbin.ElasticsearchClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;

/**
 * Created by luyb on 2017/7/6.
 */
public class ElasticsearchAddIndex {
    /**
     * ��־��
     */
    private static Logger logger = LogManager.getLogger(ElasticsearchAddIndex.class);
    /**
     * �½�����
     * @param index ������
     */
    public void createNewIndex(String index){
        try{
            if(!isExistsIndex(index)){
                ElasticsearchClient.getElasticsearchClient().admin().indices().prepareCreate(index).get();
            }else{
                logger.info("������" + index + "�Ѿ����ڲ���Ҫ������");
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    /**
     * �ж�ָ�����������Ƿ����
     * @param indexName ������
     * @return  ���ڣ�true; �����ڣ�false;
     */
    public boolean isExistsIndex(String indexName){
        IndicesExistsResponse response =
                ElasticsearchClient.getElasticsearchClient().admin().indices().exists(
                        new IndicesExistsRequest().indices(new String[]{indexName})).actionGet();
        return response.isExists();
    }

    public static void main(String[] args) {
        try{
            ElasticsearchAddIndex elasticsearchTest = new ElasticsearchAddIndex();
            String index = "20170717";
            elasticsearchTest.createNewIndex(index);
        }catch (Exception e){
            logger.error("",e);
        }finally {
            ElasticsearchClient.getElasticsearchClient().close();
        }
    }
}
