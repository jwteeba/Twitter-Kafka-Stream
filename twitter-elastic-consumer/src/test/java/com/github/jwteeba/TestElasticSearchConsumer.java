package com.github.jwteeba;

public class TestElasticSearchConsumer {
    public static void main(String[] args) {
        String configFile = "/home/blackpanther07/.dev_config/elasticsearch.conf";
        ElasticSearchConsumer elasticSearchConsumer = new ElasticSearchConsumer(configFile);
        elasticSearchConsumer.run();
    }
}
