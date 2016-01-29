package com.paradeto.sink;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by Ayou on 2015/11/22.
 */
public class KafkaSink extends AbstractSink implements Configurable {
    private static final Log logger = LogFactory.getLog(KafkaSink.class);
    private String topic;
    private Producer producer;
    public void configure(Context context) {
        // 这里写死了，不太好
        topic = "kafka";
        Map<String,String> props = new HashMap<String,String>();
        props.put("bootstrap.servers", "localhost:9092");
        //props.put("serializer.class", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // props.setProperty("producer.type", "async");
        // props.setProperty("batch.num.messages", "1");
        //props.put("request.required.acks","1");
        producer  = new KafkaProducer(props);
    }

    public Status process() throws EventDeliveryException {
        Channel channel =getChannel();
        Transaction tx =channel.getTransaction();
        try {
            tx.begin();
            Event e = channel.take();
            if(e ==null) {
                tx.rollback();
                return Status.BACKOFF;
            }
            ProducerRecord data = new ProducerRecord(topic,null,new String(e.getBody()));
            producer.send(data);
            logger.info("Message: {}"+new String( e.getBody())); tx.commit(); return Status.READY;
        }
        catch(Exception e) {
            logger.error("KafkaSinkException:{}",e);
            tx.rollback();
            return Status.BACKOFF; }
        finally {
            tx.close();
        }
    }
}
