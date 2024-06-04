package com.project.springboot;

import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;


@Service
public class WikimediaChangesProducer {

        private static final Logger LOGGER = LoggerFactory.getLogger(WikimediaChangesProducer.class);

        private KafkaTemplate<String,String> kafkaTemplate;

    public WikimediaChangesProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage() throws URISyntaxException, InterruptedException {
        String topic = "wikimedia_recent";

        // to read real time stream data from wikimedia, we use event source

        BackgroundEventHandler eventHandler = new WikimediaChangesHandler(kafkaTemplate,topic);

        String url = "https://stream.wikimedia.org/v2/stream/recentchange";

        URI uri = new URI("https://stream.wikimedia.org/v2/stream/recentchange");
        EventSource.Builder eventSourceBuilder = new EventSource.Builder(uri);

        BackgroundEventSource.Builder builder = new BackgroundEventSource.Builder(eventHandler, eventSourceBuilder);
        BackgroundEventSource eventSource = builder.build();
        eventSource.start();
        TimeUnit.MINUTES.sleep(10);
    }
}
