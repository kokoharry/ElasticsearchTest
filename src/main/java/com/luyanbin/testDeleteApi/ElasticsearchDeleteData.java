package com.luyanbin.testDeleteApi;

import com.luyanbin.ElasticsearchClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.byscroll.BulkByScrollResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryAction;


import java.io.IOException;

/**
 * Created by luyb on 2017/7/6.
 */
public class ElasticsearchDeleteData {

    /**
     * ��־��
     */
    private static Logger logger = LogManager.getLogger(ElasticsearchDeleteData.class);
    /**
     * ԭʼAPI ����client��������index��Ȼ�����type������id�����������в���
     * @param index ���� ���ݿ�
     * @param type ���� ��
     * @param id ���� ��
     * @throws IOException
     */
    private void deleteData(String index, String type, String id) throws IOException {
        DeleteResponse response = ElasticsearchClient.getElasticsearchClient().prepareDelete(index, type, id)
                .get();
        //IndexResponse�����в����ķ��ؽ��
        logger.info(response);
    }

    /**
     * ͬ��ɾ��API ���ݲ�ѯ�������ɾ������Ҫָ���̶���index�������⣩
     */
    private void deleteDataByQuery(){
        BulkByScrollResponse response =
                DeleteByQueryAction.INSTANCE.newRequestBuilder(ElasticsearchClient.getElasticsearchClient())
                        .filter(QueryBuilders.matchQuery("_type", "1499407707939"))
                        .source("20170703")
                        .get();
        logger.info(response);
    }

    /**
     * �첽ɾ��API ���ݲ�ѯ�������ɾ������Ҫָ���̶���index�������⣩
     */
    private void deleteDataByQueryAsync(){
        DeleteByQueryAction.INSTANCE.newRequestBuilder(ElasticsearchClient.getElasticsearchClient())
                .filter(QueryBuilders.matchQuery("user", "lyb"))
                .source("20170703")
                .execute(new ActionListener<BulkByScrollResponse>() {
                    @Override
                    public void onResponse(BulkByScrollResponse response) {
                        logger.info(response);
                    }
                    @Override
                    public void onFailure(Exception e) {
                        // Handle the exception
                        logger.error(e);
                    }
                });

    }


    public static void main(String[] args) {
        try{
            ElasticsearchDeleteData elasticsearchTest = new ElasticsearchDeleteData();
            String index = "20170706";
            String type = System.currentTimeMillis() +"";
            for(int i=0;i<100;i++){
//                elasticsearchTest.deleteData(index,"test_lyb",i+"");
            }
//            elasticsearchTest.deleteDataByQueryAsync();
            elasticsearchTest.deleteDataByQuery();
        }catch (Exception e){
            logger.error("",e);
        }finally {
            ElasticsearchClient.getElasticsearchClient().close();
        }
    }
}
