package com.luyanbin.testGetApi;

import com.luyanbin.ElasticsearchClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;

import java.io.IOException;

/**
 * Created by luyb on 2017/7/6.
 */
public class ElasticsearchMutilGetData {

    /**
     * ��־��
     */
    private static Logger logger = LogManager.getLogger(ElasticsearchMutilGetData.class);
    /**
     * ԭʼAPI ����client��������index��Ȼ�����type������id�����������в���
     * @param index ���� ���ݿ�
     * @param type ���� ��
     * @param id ���� ��
     * @throws IOException
     */
    private void getData(String index, String type, String id) throws IOException {
        MultiGetResponse multiGetItemResponses = ElasticsearchClient.getElasticsearchClient().prepareMultiGet()
                .add(index, type, id)
                .add(index, type, "1","2","3")
                .get();

        for (MultiGetItemResponse itemResponse : multiGetItemResponses) {
            GetResponse response = itemResponse.getResponse();
            if (response.isExists()) {
                String json = response.getSourceAsString();
                logger.info(response);
            }
        }
        //IndexResponse�����в����ķ��ؽ��
    }

    public static void main(String[] args) {
        try{
            ElasticsearchMutilGetData elasticsearchTest = new ElasticsearchMutilGetData();
            String index = "20170706";
            String type = System.currentTimeMillis() +"";
//            elasticsearchTest.getData(index,"test_lyb","1");
            elasticsearchTest.getData(index,"test_lyb","101");
        }catch (Exception e){
            logger.error("",e);
        }finally {
            ElasticsearchClient.getElasticsearchClient().close();
        }
    }
}
