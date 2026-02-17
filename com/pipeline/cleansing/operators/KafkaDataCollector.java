package com.pipeline.cleansing.collector;

import com.pipeline.cleansing.core.impl.CacheManager;
import com.pipeline.cleansing.model.DataPoint;
import com.pipeline.cleansing.model.TimeSeriesData;
import com.pipeline.cleansing.storage.SQLiteDataStorage;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Kafka数据采集消费者。
 * 从Kafka Topic实时消费PI系统推送的传感器数据，
 * 同时写入缓存（供清洗模块使用）和本地SQLite（灾备备份）。
 *
 * 消息格式约定（JSON）：
 * {"pointId":"TAG001","value":12.5,"quality":0,"timestamp":1708128000000}
 */
public class KafkaDataCollector implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(KafkaDataCollector.class);

    private final String bootstrapServers;
    private final String topic;
    private final String groupId;
    private final CacheManager cacheManager;
    private final SQLiteDataStorage dataStorage;
    private final AtomicBoolean running = new AtomicBoolean(false);

    private KafkaConsumer<String, String> consumer;

    public KafkaDataCollector(String bootstrapServers, String topic, String groupId,
                              CacheManager cacheManager, SQLiteDataStorage dataStorage) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.groupId = groupId;
        this.cacheManager = cacheManager;
        this.dataStorage = dataStorage;
    }

    public void start() {
        if (!running.compareAndSet(false, true)) {
            log.warn("KafkaDataCollector is already running.");
            return;
        }

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10000");

        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));

        Thread collectorThread = new Thread(this, "kafka-data-collector");
        collectorThread.setDaemon(true);
        collectorThread.start();

        log.info("KafkaDataCollector started. Topic: {}, Group: {}", topic, groupId);
    }

    @Override
    public void run() {
        try {
            while (running.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    try {
                        processRecord(record);
                    } catch (Exception e) {
                        log.error("Failed to process Kafka record at offset {}: {}",
                                record.offset(), e.getMessage());
                    }
                }
            }
        } catch (Exception e) {
            log.error("KafkaDataCollector encountered fatal error", e);
        } finally {
            if (consumer != null) {
                consumer.close();
            }
            log.info("KafkaDataCollector stopped.");
        }
    }

    private void processRecord(ConsumerRecord<String, String> record) {
        // 简化的JSON解析（生产环境应使用Jackson等库）
        String json = record.value();
        String pointId = extractJsonString(json, "pointId");
        double value = extractJsonDouble(json, "value");
        int quality = extractJsonInt(json, "quality");
        long timestampMs = extractJsonLong(json, "timestamp");

        if (pointId == null) return;

        DataPoint dp = new DataPoint(new Timestamp(timestampMs), value, quality);

        // 双路并行：写入缓存 + 写入本地存储
        cacheManager.loadData(pointId, wrapSinglePoint(pointId, dp));
        cacheManager.invalidateResults(pointId); // 源数据更新，使依赖的结果缓存失效

        dataStorage.writeTimeSeriesPoint(pointId, dp);
    }

    private TimeSeriesData wrapSinglePoint(String pointId, DataPoint dp) {
        TimeSeriesData ts = new TimeSeriesData(pointId);
        ts.addDataPoint(dp);
        return ts;
    }

    public void stop() {
        running.set(false);
    }

    // ---- 简化的JSON解析辅助方法 ----

    private String extractJsonString(String json, String key) {
        String pattern = "\"" + key + "\":\"";
        int start = json.indexOf(pattern);
        if (start < 0) return null;
        start += pattern.length();
        int end = json.indexOf("\"", start);
        return (end > start) ? json.substring(start, end) : null;
    }

    private double extractJsonDouble(String json, String key) {
        String pattern = "\"" + key + "\":";
        int start = json.indexOf(pattern);
        if (start < 0) return 0.0;
        start += pattern.length();
        int end = start;
        while (end < json.length() && (Character.isDigit(json.charAt(end))
                || json.charAt(end) == '.' || json.charAt(end) == '-')) {
            end++;
        }
        return Double.parseDouble(json.substring(start, end));
    }

    private int extractJsonInt(String json, String key) {
        return (int) extractJsonDouble(json, key);
    }

    private long extractJsonLong(String json, String key) {
        return (long) extractJsonDouble(json, key);
    }
}
