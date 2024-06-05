package com.project.springboot;

import com.project.springboot.entity.WikimediaData;
import com.project.springboot.repository.WikimediaDataRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaDatabaseConsumer {

    private WikimediaDataRepository dataRepository;
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaDatabaseConsumer.class);

    public KafkaDatabaseConsumer(WikimediaDataRepository dataRepository){
        this.dataRepository=dataRepository;
    }
    @KafkaListener(topics = "wikimedia_recent",groupId = "myGroup")
    public void consume(String eventMessage){
          LOGGER.info(String.format("event Message received -> %s", eventMessage));

          WikimediaData wikimediaData = new WikimediaData();
          wikimediaData.setWikiEventData(eventMessage);

          dataRepository.save(wikimediaData);
    }
}
