package com.tencent.kafka;

import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * @author horizonliu
 * @date 2019/2/25 3:20 PM
 */
@Slf4j
public class Main {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "horizonliu-test");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Arrays.asList("tbox_period_1s", "tbox_period_5s",
                "tbox_period_30s","tbox_period_60s", "tbox_event_msg"));

        Gson gson = new Gson();
        File filename = new File("/root/horizonliu/tbox_data.txt");
        BufferedWriter writer = null;

        try {
            filename.createNewFile();
            writer = new BufferedWriter(new FileWriter(filename));
        } catch (IOException ex) {
            System.out.println("fail to create file");
            ex.printStackTrace();
        }

        if (writer == null) {
            System.out.println("fail create file writer");
            System.exit(-1);
        }


        try {
            //1)
            while (true) {
                //2)
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(100));
                //3)
                for (ConsumerRecord<String, String> record : records)
                {
                    log.debug("topic = %s, partition = %s, offset = %d, customer = %s, country = %s\n",
                            record.topic(),
                            record.partition(),
                            record.offset(),
                            record.key(),
                            record.value());

                    // 将结果保存在文本文件中
                    String objectStr = gson.toJson(record);
                    writer.write(objectStr + "\n\n");
                }
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            consumer.close(); //4
        }

        try {
            writer.close();
        } catch (IOException ex) {
            System.out.println("close file stream fail");
            ex.printStackTrace();
        }
    }
}
