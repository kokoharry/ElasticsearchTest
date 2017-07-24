package com.luyanbin.testUpdateApi;

import com.luyanbin.ElasticsearchClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.byscroll.BulkByScrollResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryAction;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by luyb on 2017/7/6.
 */
public class ElasticsearchUpdateData {

    /**
     * 日志类
     */
    private static Logger logger = LogManager.getLogger(ElasticsearchUpdateData.class);
    /**
     * 原始API 根据client或者索引index，然后根据type（表），id（主键）进行更新数据
     * @param index 索引 数据库
     * @param type 类型 表
     * @param id 主键 键
     * @throws IOException
     */
    private void updateData(String index, String type, String id) throws IOException, ExecutionException, InterruptedException {
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index(index);
        updateRequest.type(type);
        updateRequest.id(id);
        updateRequest.doc(jsonBuilder()
                .startObject()
                .field("user", "wx")
                .endObject());
        UpdateResponse response = ElasticsearchClient.getElasticsearchClient().update(updateRequest).get();
        logger.info(response);
    }

    public static void main(String[] args) {
        try{
            ElasticsearchUpdateData elasticsearchTest = new ElasticsearchUpdateData();
            String index = "20170703";
            String type = System.currentTimeMillis() +"";
            for(int i=0;i<100;i++){
//                elasticsearchTest.deleteData(index,"test_lyb",i+"");
            }
            elasticsearchTest.updateData(index,"1499407707939","1");
        }catch (Exception e){
            logger.error("",e);
        }finally {
            ElasticsearchClient.getElasticsearchClient().close();
        }
    }
}
