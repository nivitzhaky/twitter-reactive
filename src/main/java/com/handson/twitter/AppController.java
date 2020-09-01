package com.handson.twitter;

import com.handson.twitter.kafka.AppKafkaSender;
import com.handson.twitter.kafka.KafkaListener;
import com.handson.twitter.nlp.AppSentiment;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import twitter4j.TwitterException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Timer;
import java.util.TimerTask;

import static com.handson.twitter.config.KafkaEmbeddedConfig.TEST_TOPIC;
import static com.handson.twitter.kafka.KafkaListener.FINISH_PROCESSING;


@RestController
public class AppController {
    public static final int STOP_DELAY = 120000;
    AppTwitterStream twitter;

    @Autowired
    private AppKafkaSender kafkaSender;

    KafkaListener kafka = null;
    @RequestMapping(path = "/hello", method = RequestMethod.GET)
    public  @ResponseBody Mono<String> hello()  {
        return Mono.just("Hello");
    }


    @RequestMapping(path = "/twitter", method = RequestMethod.GET)
    public  @ResponseBody Flux<String> twitter(@RequestParam String keyword, @RequestParam String mode,
                                               @RequestParam Integer timeWindow, @RequestParam boolean sentiment) throws TwitterException {
        AppSentiment analyzer = new AppSentiment();
        handleStopIfNeeded();
        AppTwitterStream twitterStream =  new AppTwitterStream();
        kafka = new KafkaListener();
        this.twitter = twitterStream;
        twitterStream.filter(keyword).map((x)-> kafkaSender.send(x, TEST_TOPIC)).subscribe();

        if(sentiment){
            return kafka.listen(TEST_TOPIC).map(x-> new TimeAndMessage(DateTime.now(),x))
                    .window(Duration.ofSeconds(timeWindow))
                    .flatMap(window->toArrayList(window))
                    .map(items->{
                        Double avg = items.stream().map(x->analyzer.analyze(x.message)).mapToDouble(y->y).average().orElse(0.0);
                        if (items.size() == 0) return "EMPTY<br>";
                        return items.get(0).cur.toString() + "->" +  items.size() + " messages, sentiment = " + avg +  "<br>";
                    });
        }else if (mode.equals("grouped")){
            return kafka.listen(TEST_TOPIC).map(x-> new TimeAndMessage(DateTime.now(),x))
                    .window(Duration.ofSeconds(timeWindow))
                    .flatMap(window->toArrayList(window))
                    .map(y->{
                        if (y.size() == 0) return "size: 0 <br>";
                        return y.get(0).cur.toString() + "size: " + y.size() + "<br>";
                    });
        }else {
            return  kafka.listen(TEST_TOPIC).map(x-> x + "<br>");
        }
    }

    private void handleStopIfNeeded() {
        scheduleStreamStop();
        if (kafka != null) kafka.stopListen();
        if (this.twitter != null) this.twitter.shutdown();
    }

    private void scheduleStreamStop() {
        Timer t = new Timer();
        StopTask stop = new StopTask(this);
        t.schedule(stop, STOP_DELAY);
    }

    @RequestMapping(path = "/stop", method = RequestMethod.GET)
    public Mono<String> stopTwitter(){
        twitter.shutdown();
        kafkaSender.send(FINISH_PROCESSING, TEST_TOPIC);

        return Mono.just("ok");
    }

    public static <T> Mono<ArrayList<T>> toArrayList(Flux<T> source) {
        return  source.reduce(new ArrayList(), (a, b) -> { a.add(b);return a; });
    }

    class TimeAndMessage {
        DateTime cur;
        String message;

        public TimeAndMessage(DateTime cur, String message) {
            this.cur = cur;
            this.message = message;
        }



    }
    class StopTask extends TimerTask {
        AppController controller;
        public StopTask(AppController controller) {
            this.controller = controller;
        }
        public void run() {
            controller.stopTwitter();
        }
    }
}