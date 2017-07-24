package com.luyanbin.testSearchApi;

import com.luyanbin.ElasticsearchClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;

/**
 * Created by luyb on 2017/7/6.
 */
public class ElasticsearchSearchData {

    /**
     * 日志类
     */
    private static Logger logger = LogManager.getLogger(ElasticsearchSearchData.class);
    /**
     * 原始API 根据client或者索引index，然后根据type（表），id（主键）进行插入
     * @throws IOException
     */
    private void searchData() throws Exception {

        QueryBuilder qb = QueryBuilders.termQuery("user", "lyb");
        SearchResponse scrollResp = ElasticsearchClient.getElasticsearchClient().prepareSearch("20170717")
                .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
                .setScroll(new TimeValue(60000))
                .setQuery(qb)
                .setSize(100).get();
                //max of 100 hits will be returned for each scroll
                //Scroll until no hits are returned
        do {
            System.out.println(scrollResp.getHits().getTotalHits());
            for (SearchHit hit : scrollResp.getHits().getHits()) {
                //Handle the hit...
                logger.info(hit.getSource());
            }
            scrollResp = ElasticsearchClient.getElasticsearchClient().prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
        } while(scrollResp.getHits().getHits().length != 0); // Zero hits mark the end of the scroll and the while loop.
    }



    public static void main(String[] args) {
        try{
            ElasticsearchSearchData elasticsearchTest = new ElasticsearchSearchData();
            String index = "20170706";
            String type = System.currentTimeMillis() +"";
//            elasticsearchTest.getData(index,"test_lyb","1");
            elasticsearchTest.searchData();
        }catch (Exception e){
            logger.error("",e);
        }finally {
            ElasticsearchClient.getElasticsearchClient().close();
        }
    }
}
