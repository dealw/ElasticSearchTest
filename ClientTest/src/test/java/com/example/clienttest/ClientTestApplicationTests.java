package com.example.clienttest;

import com.alibaba.fastjson.JSON;
import com.example.clienttest.pojo.entity.User;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@SpringBootTest
class ClientTestApplicationTests {
    @Resource
    @Qualifier("restHighLevelClient")
    private RestHighLevelClient restHighLevelClient;

    @Test
    void createIndex() throws IOException {
        CreateIndexRequest indexRequest = new CreateIndexRequest("ougaho");
        CreateIndexResponse createIndexResponse = restHighLevelClient.indices().create(indexRequest, RequestOptions.DEFAULT);
        System.out.println(createIndexResponse);
    }

    @Test
    void deleteIndex() throws IOException {
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("ougaho");
        AcknowledgedResponse delete = restHighLevelClient.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
        System.out.println(delete);
    }

    @Test
    void addDocument() throws IOException {
        User ougaho = new User("deal", 22);

        IndexRequest request = new IndexRequest("ougaho");

        request.id("1001");
        request.source(JSON.toJSONString(ougaho), XContentType.JSON);

        IndexResponse indexResponse = restHighLevelClient.index(request, RequestOptions.DEFAULT);
        System.out.println(indexResponse);
    }

    @Test
    void isExists() throws IOException {
        GetRequest request = new GetRequest("ougaho", "1001");
        request.fetchSourceContext(new FetchSourceContext(false));

        boolean exists = restHighLevelClient.exists(request, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    @Test
    void getDocument() throws IOException {
        GetRequest request = new GetRequest("ougaho", "1001");
        GetResponse documentFields = restHighLevelClient.get(request, RequestOptions.DEFAULT);
        System.out.println(documentFields.toString());
    }

    @Test
    void updateDocument() throws IOException {
        UpdateRequest request = new UpdateRequest("ougaho", "1001");

        User user = new User("dealee", 18);
        request.doc(JSON.toJSONString(user), XContentType.JSON);

        UpdateResponse update = restHighLevelClient.update(request, RequestOptions.DEFAULT);
        System.out.println(update);
    }

    @Test
    void deleteDocument() throws IOException {
        DeleteRequest request = new DeleteRequest("ougaho", "1001");
        DeleteResponse delete = restHighLevelClient.delete(request, RequestOptions.DEFAULT);
        System.out.println(delete);
    }

    @Test
    void addBulkRequest() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout("10s");

        List<User> users = new ArrayList<>();
        users.add(new User("ougaho", 22));
        users.add(new User("ougaho1", 23));
        users.add(new User("ougaho2", 24));
        users.add(new User("ougaho3", 25));
        users.add(new User("ougaho4", 22));
        users.add(new User("ougaho5", 22));
        users.add(new User("ougaho6", 22));

        for (int i = 1; i <= users.size(); i++) {
            bulkRequest.add(
                    new IndexRequest("ougaho")
                            .id("100" + i)
                            .source(JSON.toJSONString(users.get(i-1)), XContentType.JSON)
            );
        }

        BulkResponse bulk = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(!bulk.hasFailures());
    }

    @Test
    void addBulkDeleteRequest() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout("10s");

        List<User> users = new ArrayList<>();
        users.add(new User("ougaho", 22));
        users.add(new User("ougaho1", 23));
        users.add(new User("ougaho2", 24));
        users.add(new User("ougaho3", 25));
        users.add(new User("ougaho4", 22));
        users.add(new User("ougaho5", 22));
        users.add(new User("ougaho6", 22));

        for (int i = 1; i <= users.size(); i++) {
            bulkRequest.add(
                    new IndexRequest("ougaho")
                            .id("100" + i)
                            .source(JSON.toJSONString(users.get(i-1)),XContentType.JSON)
            );
        }

        BulkResponse bulk = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(!bulk.hasFailures());
    }

    @Test
    void searchRequest() throws IOException {
        SearchRequest searchRequest = new SearchRequest();

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name", "ougaho1");
        searchSourceBuilder.query(termQueryBuilder);
        searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

        searchRequest.source(searchSourceBuilder);
        SearchResponse search = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        for (SearchHit hit : search.getHits()) {
            System.out.println(hit.getSourceAsMap().toString());
        }
    }

}
