package com.github.jwteeba;

public class TestProducer {
    public static void main(String[] args) {
        TwitterProducer producer = new TwitterProducer("/path/to/CONFIG");
        producer.run();
    }
}
