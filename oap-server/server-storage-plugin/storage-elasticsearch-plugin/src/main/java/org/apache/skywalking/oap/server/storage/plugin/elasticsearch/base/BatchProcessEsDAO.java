/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.skywalking.oap.server.storage.plugin.elasticsearch.base;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.skywalking.oap.server.core.Const;
import org.apache.skywalking.oap.server.core.storage.IBatchDAO;
import org.apache.skywalking.oap.server.library.client.elasticsearch.ElasticSearchClient;
import org.apache.skywalking.oap.server.library.client.request.InsertRequest;
import org.apache.skywalking.oap.server.library.client.request.PrepareRequest;
import org.apache.skywalking.oap.server.library.util.CollectionUtils;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class BatchProcessEsDAO extends EsDAO implements IBatchDAO {

    private static final Logger logger = LoggerFactory.getLogger(BatchProcessEsDAO.class);

    private BulkProcessor bulkProcessor;
    private final int bulkActions;
    private final int flushInterval;
    private final int concurrentRequests;
    private KafkaProducer kafkaProducer;
    private String topicName;
    private boolean useKafka;
    private String nameSpace;

    private static Gson gson;

    static {
        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.disableHtmlEscaping();
        gson = gsonBuilder.create();
    }

    public BatchProcessEsDAO(ElasticSearchClient client, int bulkActions, int flushInterval, int concurrentRequests) {
        super(client);
        this.bulkActions = bulkActions;
        this.flushInterval = flushInterval;
        this.concurrentRequests = concurrentRequests;
    }

    public BatchProcessEsDAO(ElasticSearchClient client, int bulkActions, int flushInterval, int concurrentRequests,
                             boolean useKafka, KafkaProducer kafkaProducer, String topicName, String nameSpace) {
        super(client);
        this.bulkActions = bulkActions;
        this.flushInterval = flushInterval;
        this.concurrentRequests = concurrentRequests;
        this.kafkaProducer = kafkaProducer;
        this.topicName = topicName;
        this.useKafka = useKafka;
        this.nameSpace = nameSpace;
    }

    @Override
    public void asynchronous(InsertRequest insertRequest) {
        if (bulkProcessor == null) {
            this.bulkProcessor = getClient().createBulkProcessor(bulkActions, flushInterval, concurrentRequests);
        }

        IndexRequest indexRequest = (IndexRequest) insertRequest;
        String name = indexRequest.index();
        if (useKafka && name != null && name.startsWith(nameSpace + Const.ID_CONNECTOR + "segment")) {
            String id = indexRequest.id();
            Map<String, Object> map = indexRequest.sourceAsMap();
            ProducerRecord producerRecord = new ProducerRecord(topicName, id, gson.toJson(map));
            kafkaProducer.send(producerRecord);
        }
        this.bulkProcessor.add((IndexRequest) insertRequest);
    }

    @Override
    public void synchronous(List<PrepareRequest> prepareRequests) {
        if (CollectionUtils.isNotEmpty(prepareRequests)) {
            BulkRequest request = new BulkRequest();

            for (PrepareRequest prepareRequest : prepareRequests) {
                if (prepareRequest instanceof InsertRequest) {
                    IndexRequest indexRequest = (IndexRequest) prepareRequest;
                    String name = indexRequest.index();
                    if (useKafka && name != null && name.startsWith(nameSpace + Const.ID_CONNECTOR + "segment")) {
                        String id = indexRequest.id();
                        Map<String, Object> map = indexRequest.sourceAsMap();
                        ProducerRecord producerRecord = new ProducerRecord(topicName, id, gson.toJson(map));
                        kafkaProducer.send(producerRecord);
                    }
                    request.add((IndexRequest) prepareRequest);
                } else {
                    UpdateRequest updateRequest = (UpdateRequest) prepareRequest;
                    String name = updateRequest.index();
                    if (useKafka && name != null && name.startsWith(nameSpace + Const.ID_CONNECTOR + "segment")) {
                        String id = updateRequest.id();
                        Map<String, Object> map = updateRequest.doc().sourceAsMap();
                        ProducerRecord producerRecord = new ProducerRecord(topicName, id, gson.toJson(map));
                        kafkaProducer.send(producerRecord);
                    }
                    request.add((UpdateRequest) prepareRequest);
                }
            }
            getClient().synchronousBulk(request);
        }
    }
}
