package com.github.jwteeba;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;


public class TwitterProducer {
    private final Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());
    private final List<String> terms = Lists.newArrayList("messi"); // track terms
    private String oauth_consumer_key;
    private String oauth_consumer_secret;
    private String oauth_token;
    private String oauth_token_secret;
    private final String CONFIGFILE;

    public TwitterProducer(String file){
        this.CONFIGFILE = file;

    }

    protected void run(){
        logger.info("Setting up things...");

        /**
         * Set up your blocking queues: Be sure to size these properly
         * based on expected TPS of your stream
         * */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

        // Create Twitter client
        Client client = createTwitterClient(msgQueue);
        client.connect();

        // Create Kafka Producer
        KafkaProducer<String, String> producer = createKafkaProducer();

        // Create shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() ->{
            logger.info("Stopping Application");
            logger.info("Shutting down client from twitter");
            client.stop();
            logger.info("Closing Producing");
            producer.close(); // Send all messages store in memory before closing
            logger.info("Done!!!");

        }));

        // loop to send tweets to kafka
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                // e.printStackTrace();
                logger.error("ERROR: ", e.getMessage());
                client.stop();
            }

            if(msg != null){
               logger.info(msg);
               producer.send(new ProducerRecord<>("twitter-tweets", null, msg), new Callback(){

                   @Override
                   public void onCompletion(RecordMetadata metadata, Exception exception) {
                       if(exception != null){
                           logger.error("ERROR: ", exception.getMessage());
                       }
                   }
               });
            }
        }
        logger.info("End of Application");
    }


    private Client createTwitterClient(BlockingQueue<String> msgQueue){

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        hosebirdEndpoint.trackTerms(terms);

        // Get twitter configs value
        Properties properties = new Properties();
        try {
            InputStream is = new FileInputStream(CONFIGFILE);
            properties.load(is);
            oauth_consumer_key = properties.getProperty("oauth_consumer_key");
            oauth_consumer_secret = properties.getProperty("oauth_consumer_secret");
            oauth_token_secret = properties.getProperty("oauth_token_secret");
            oauth_token = properties.getProperty("oauth_token");

        } catch (FileNotFoundException e) {
            System.out.println("FileNotFoundException: "+e.getMessage());
        } catch (IOException e) {
            logger.error("Json Parsing ERROR: ", e.getMessage());
        }

        Authentication hosebirdAuth = new OAuth1(oauth_consumer_key,  oauth_consumer_secret, oauth_token, oauth_token_secret);

        // Create client:
        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01") // for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();
        return hosebirdClient;

    }

    private KafkaProducer<String, String> createKafkaProducer() {
        String bootStrapServer = "127.0.0.1:9092";

        // Create Producer properties
        Properties producerProperties = new Properties();
        producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Safe producer properties
        producerProperties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        producerProperties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        producerProperties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        producerProperties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        // High throughput producer properties ( At the expense of a bit of latency and CPU usage)
        producerProperties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        producerProperties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        producerProperties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024)); // 32KB Batch Size

        return new KafkaProducer(producerProperties);
    }
}
