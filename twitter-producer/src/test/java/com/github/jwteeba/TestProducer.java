package com.github.jwteeba;

public class TestProducer {
    public static void main(String[] args) {
        TwitterProducer producer = new TwitterProducer("/home/blackpanther07/.config/twitter-configs.conf");
        producer.run();
    }
}
