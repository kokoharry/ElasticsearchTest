package com.luyanbin.testBulkApi;

import com.luyanbin.ElasticsearchClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.util.*;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by luyb on 2017/7/6.
 */
public class ElasticsearchBulk {

    /**
     * 日志类
     */
    private static Logger logger = LogManager.getLogger(ElasticsearchBulk.class);
    /**
     * 原始API 根据client或者索引index，然后根据type（表），id（主键）进行插入
     */
    private BulkProcessor getBulk(){
        BulkProcessor bulkProcessor = BulkProcessor.builder(
                ElasticsearchClient.getElasticsearchClient(),
                new BulkProcessor.Listener() {

                    /**
                     * 执行批量提交前
                     * @param executionId
                     * @param request
                     */
                    @Override
                    public void beforeBulk(long executionId,
                                           BulkRequest request) {
                        logger.info("本次提交数量"+request.numberOfActions());
                    }

                    /**
                     * 执行批量提交成功后
                     * @param executionId
                     * @param request
                     * @param response
                     */
                    @Override
                    public void afterBulk(long executionId,
                                          BulkRequest request,
                                          BulkResponse response) {

                    }

                    /**
                     * 执行批量体检出现异常后
                     * @param executionId
                     * @param request
                     * @param failure
                     */
                    @Override
                    public void afterBulk(long executionId,
                                          BulkRequest request,
                                          Throwable failure) {

                    }
                })
                .setBulkActions(10000) //批量条数
                .setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB)) //批量大小
                .setFlushInterval(TimeValue.timeValueSeconds(5)) //批量时间
                .setConcurrentRequests(1)
                .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
                .build();
        return bulkProcessor;
    }

    private void addDataByBulkProcess(List<Map<String,String>> list) throws IOException {

        BulkProcessor bulkProcessor = getBulk();
        for(Map<String,String> map : list){
            bulkProcessor.add(new IndexRequest(map.get("index"),
                    map.get("type"), map.get("id")).source(jsonBuilder()
                    .startObject()
                    .field("user", "lyb")
                    .field("postDate", new Date())
                    .field("message", "trying out Elasticsearch by BULK")
                    .endObject())
            );
        }
    }

    public static void main(String[] args) {
        try{
            ElasticsearchBulk elasticsearchTest = new ElasticsearchBulk();
            String index = "20170717";
            String type = System.currentTimeMillis() +"";
            List<Map<String,String>> list = new ArrayList<>();
            for(int i=0;i<10;i++){
                Map<String,String> map = new HashMap<>();
                map.put("index",index);
                map.put("type",type + "bulkProcess");
                map.put("id",i+"");
                list.add(map);
            }
            elasticsearchTest.addDataByBulkProcess(list);
            Thread.sleep(10000);
        }catch (Exception e){
            logger.error("",e);
        }finally {
            ElasticsearchClient.getElasticsearchClient().close();
        }
    }
}
