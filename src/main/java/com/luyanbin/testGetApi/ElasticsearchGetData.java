package com.luyanbin.testGetApi;

import com.luyanbin.ElasticsearchClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;

import java.io.IOException;
import java.util.*;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by luyb on 2017/7/6.
 */
public class ElasticsearchGetData {

    /**
     * ��־��
     */
    private static Logger logger = LogManager.getLogger(ElasticsearchGetData.class);
    /**
     * ԭʼAPI ����client��������index��Ȼ�����type������id�����������в���
     * @param index ���� ���ݿ�
     * @param type ���� ��
     * @param id ���� ��
     * @throws IOException
     */
    private void getData(String index, String type, String id) throws IOException {
        GetResponse response = ElasticsearchClient.getElasticsearchClient().prepareGet(index, type, id).get();
        //IndexResponse�����в����ķ��ؽ��
        logger.info(response);
    }

    /**
     * ��ɾ��api��ͬһ���ڵ���ִ��ʱ����һ����Ƭ��ִ��һ��api����䵽ͬһ���������ϣ���
     * ɾ��api����ִ��ǰ�����߳�ģʽ��operationThreadedѡ���
     * operationThreaded���ѡ����ʹ�������������һ���߳���ִ�У�
     * ����һ������������̣߳��������api�����첽�ģ���ִ�С�
     * Ĭ�ϵĻ�operationThreaded�����ó�true��
     * ����ζ�������������һ����ͬ���߳���ִ��
     * @param index ���� ���ݿ�
     * @param type ���� ��
     * @param id ���� ��
     * @throws IOException
     */
    private void getDataSetOperationThreaded(String index, String type, String id) throws IOException {
        GetResponse response = ElasticsearchClient.getElasticsearchClient().prepareGet(index, type, id)
                .setOperationThreaded(false).get();
        //IndexResponse�����в����ķ��ؽ��
        logger.info(response);
    }


    public static void main(String[] args) {
        try{
            ElasticsearchGetData elasticsearchTest = new ElasticsearchGetData();
            String index = "20170706";
            String type = System.currentTimeMillis() +"";
//            elasticsearchTest.getData(index,"test_lyb","1");
            elasticsearchTest.getDataSetOperationThreaded(index,"test_lyb","1");
        }catch (Exception e){
            logger.error("",e);
        }finally {
            ElasticsearchClient.getElasticsearchClient().close();
        }
    }
}
