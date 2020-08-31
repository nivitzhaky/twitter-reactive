package com.handson.twitter;


import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import twitter4j.*;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

public class AppTwitterStream implements StatusListener {

    EmitterProcessor<String> emitterProcessor;
    public TwitterStream twitterStream;

    public AppTwitterStream() throws TwitterException {
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true);
        cb.setOAuthConsumerKey("0DiddNspuf8WTpoxa6jb5JZ52");
        cb.setOAuthConsumerSecret("Dd8rk3jMhEhw6bpeHFoNGAbqGq9hqULii7sAPdXChDDCQWgV6b");
        cb.setOAuthAccessToken("702027578438250497-F5FaMMrYtwAdpUJw7Qh8AgaJ7wRXmRj");
        cb.setOAuthAccessTokenSecret("p6t5q8tB7cdB5zlYd5j8xpoS217v5EeXVzwUEsIkMcg9w");
        Configuration conf = cb.build();
        twitterStream = new TwitterStreamFactory(conf).getInstance();
    }

    public Flux<String> filter(String key) {
        FilterQuery filterQuery = new FilterQuery();
        String[] keys = {key};
        filterQuery.track(keys);
        twitterStream.addListener(this);
        twitterStream.filter(filterQuery);
        emitterProcessor = EmitterProcessor.create();
        emitterProcessor.map(x->x);
        return emitterProcessor;
    }

    @Override
    public void onStatus(Status status) {
        emitterProcessor.onNext(status.getText());
    }

    @Override
    public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {

    }

    @Override
    public void onTrackLimitationNotice(int i) {

    }

    @Override
    public void onScrubGeo(long l, long l1) {

    }

    @Override
    public void onStallWarning(StallWarning stallWarning) {

    }

    @Override
    public void onException(Exception e) {

    }

    public void shutdown() {
        this.twitterStream.shutdown();
        emitterProcessor.onComplete();
    }
}
