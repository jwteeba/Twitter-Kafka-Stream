package com.github.jwteeba;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {
    private String hostname;
    private String username;
    private String password;
    private String configFile;
    private final Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

    public ElasticSearchConsumer(String file){
        this.configFile = file;
    }

    public KafkaConsumer<String, String> createConsumer(String topic){
        String bootstrap_server = "127.0.0.1:9092";
        String group_id = "kafka-elasticsearch";

        // Create Consumer Config
        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_server);
        prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group_id);
        prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Create Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(prop);
        // Subscribe consumer to topic(s)
        consumer.subscribe(Arrays.asList(topic));
        return consumer;

    }

    public RestHighLevelClient createClient(){
        Properties properties = new Properties();
        try {
            InputStream is = new FileInputStream(configFile);
            properties.load(is);
            hostname = properties.getProperty("hostname");
            username = properties.getProperty("username");
            password = properties.getProperty("password");

        } catch (FileNotFoundException e) {
            System.out.println("FileNotFoundException: "+e.getMessage());
        } catch (IOException e) {
            System.out.println("IOException: "+e.getMessage());
        }
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

        RestClientBuilder requestBuilder = RestClient.builder(new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback(){
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });
        RestHighLevelClient client = new RestHighLevelClient(requestBuilder);
        return client;
    }

    private String extractIDFromTweet(String tweetJson){
        String jsonObject = JsonParser.parseString(tweetJson)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
        return jsonObject;
    }

    protected void run(){
        RestHighLevelClient client = createClient();
        try {
            KafkaConsumer<String, String> consumer = createConsumer("twitter-tweets");
            // Poll for data
            while(true){
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(100));
                for(ConsumerRecord<String, String> record: records){
                    String id = extractIDFromTweet(record.value());
                    // Insert data into elasticsearch
                    IndexRequest indexRequest = new IndexRequest("twitter").source(record.value(), XContentType.JSON);
                    indexRequest.id(id);

                    IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                    logger.info(indexResponse.getId());
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
            // client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}

