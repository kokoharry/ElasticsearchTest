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
     * 日志类
     */
    private static Logger logger = LogManager.getLogger(ElasticsearchGetData.class);
    /**
     * 原始API 根据client或者索引index，然后根据type（表），id（主键）进行插入
     * @param index 索引 数据库
     * @param type 类型 表
     * @param id 主键 键
     * @throws IOException
     */
    private void getData(String index, String type, String id) throws IOException {
        GetResponse response = ElasticsearchClient.getElasticsearchClient().prepareGet(index, type, id).get();
        //IndexResponse对象有操作的返回结果
        logger.info(response);
    }

    /**
     * 当删除api在同一个节点上执行时（在一个分片中执行一个api会分配到同一个服务器上），
     * 删除api允许执行前设置线程模式（operationThreaded选项），
     * operationThreaded这个选项是使这个操作在另外一个线程中执行，
     * 或在一个正在请求的线程（假设这个api仍是异步的）中执行。
     * 默认的话operationThreaded会设置成true，
     * 这意味着这个操作将在一个不同的线程中执行
     * @param index 索引 数据库
     * @param type 类型 表
     * @param id 主键 键
     * @throws IOException
     */
    private void getDataSetOperationThreaded(String index, String type, String id) throws IOException {
        GetResponse response = ElasticsearchClient.getElasticsearchClient().prepareGet(index, type, id)
                .setOperationThreaded(false).get();
        //IndexResponse对象有操作的返回结果
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
