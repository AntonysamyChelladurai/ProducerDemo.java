package com.anto.kafka.demo1;

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

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class ProducerDemo {
    Logger logger = LoggerFactory.getLogger(ProducerDemo.class.getName());
    String consumerKey = "o9q3zplTHhxWxxrEJOkmfRhMf";
    String consumerSecret = "iZ3x2Z66sXp7ChRXxJ7kMURRlB64KMgrEQePvsZNuB6Py8SrG8";
    String token = "1449735698899030024-5oFvkJwNNaGaysjH0rCgFks8sClKk8";
    String secret = "97XtaM5iPi9NYbiU9YPd7vwCeX2QQ4N0lYMIkJqVM1TJe";
    List<String> terms = Lists.newArrayList("kafka");

    public ProducerDemo() {
    }

    public static void main(String[] args) {
        System.out.println("Hello Antony Producer App .... ");
        // create a twitter client
        // create a kafka producer
        // loop to send tweet to kafka
        new ProducerDemo().run();
    }

    public void run() {
        // create a Twitter client
        logger.info("Run Method begins....");
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
        Client client = createTwitterClient(msgQueue);
        client.connect();
        // create a kafka producer

        KafkaProducer<String,String> buadailydata_prod = createbuaProducer();

        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.MILLISECONDS);

            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if (msg != null) {
                buadailydata_prod.send(new ProducerRecord<String, String>("TestTopics", null, msg),
                        new Callback() {
                            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                                if(e!=null){
                                    logger.error("Error at producer send :: "+e.toString());
                                }
                            }
                        });
                logger.info("Kafka Message ::  " +msg);
            }
           /* something(msg);
            profit();*/
        }
        logger.info("ENd of the Application :: ");

        // loop to send tweet to kafka
    }

    private KafkaProducer<String, String> createbuaProducer() {
        String bootstrapServers = "127.0.0.1:9092";

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<String, String>(properties);
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) {
        /* Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream
         *
         * */


        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
// Optional: set up some followings and track terms
        //List<Long> followings = Lists.newArrayList(1234L, 566788L);
        //List<Long> followings = Lists.newArrayList("twitter", "api");

        //hosebirdEndpoint.followings(followings);
        hosebirdEndpoint.trackTerms(terms);

// These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));
        // optional: use this if you want to process client events

        Client hosebirdClient = builder.build();

        return hosebirdClient;
// Attempts to establish a connection.
        //hosebirdClient.connect();

    }

}
