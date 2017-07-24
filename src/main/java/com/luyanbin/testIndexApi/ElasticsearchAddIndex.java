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
     * 日志类
     */
    private static Logger logger = LogManager.getLogger(ElasticsearchAddIndex.class);
    /**
     * 新建索引
     * @param index 索引名
     */
    public void createNewIndex(String index){
        try{
            if(!isExistsIndex(index)){
                ElasticsearchClient.getElasticsearchClient().admin().indices().prepareCreate(index).get();
            }else{
                logger.info("索引：" + index + "已经存在不需要创建了");
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    /**
     * 判断指定的索引名是否存在
     * @param indexName 索引名
     * @return  存在：true; 不存在：false;
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
