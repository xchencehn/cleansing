package com.pipeline.cleansing;

import java.io.FileInputStream;
import java.util.Properties;

/**
 * 应用配置类。
 * 对应配置文件中的系统级参数。
 */
public class AppConfig {

    // ---- 微批调度 ----
    private long microBatchIntervalMs = 500;
    private int workerParallelism = Runtime.getRuntime().availableProcessors();
    private long taskTimeoutMs = 400;

    // ---- 存储 ----
    private String storageRoot = "data/storage";
    private long timeWindowMs = 24 * 60 * 60 * 1000L;

    // ---- 缓存 ----
    private int cacheDefaultTTL = 60;         // 60个微批周期（30秒）
    private int cacheMaxPoints = 1_000_000;
    private int writeBackQueueSize = 100_000;
    private int writeBackRateLimit = 100;      // 每秒100批次

    // ---- 资源管理 ----
    private double memoryWarningThreshold = 0.70;
    private double memoryCriticalThreshold = 0.85;
    private double cpuWarningThreshold = 0.80;

    // ---- Kafka ----
    private String kafkaBootstrapServers = "localhost:9092";
    private String kafkaInputTopic = "pipeline-raw-data";
    private String kafkaOutputTopic = "pipeline-cleansed-data";
    private String kafkaGroupId = "cleansing-engine";

    public static AppConfig load(String configPath) {
        AppConfig config = new AppConfig();
        try {
            Properties props = new Properties();
            props.load(new FileInputStream(configPath));

            config.microBatchIntervalMs = Long.parseLong(
                    props.getProperty("microbatch.interval.ms", "500"));
            config.workerParallelism = Integer.parseInt(
                    props.getProperty("worker.parallelism",
                            String.valueOf(Runtime.getRuntime().availableProcessors())));
            config.taskTimeoutMs = Long.parseLong(
                    props.getProperty("task.timeout.ms", "400"));
            config.storageRoot = props.getProperty("storage.root", "data/storage");
            config.timeWindowMs = Long.parseLong(
                    props.getProperty("storage.time.window.ms",
                            String.valueOf(24 * 60 * 60 * 1000L)));
            config.cacheDefaultTTL = Integer.parseInt(
                    props.getProperty("cache.default.ttl", "60"));
            config.cacheMaxPoints = Integer.parseInt(
                    props.getProperty("cache.max.points", "1000000"));
            config.writeBackQueueSize = Integer.parseInt(
                    props.getProperty("cache.writeback.queue.size", "100000"));
            config.writeBackRateLimit = Integer.parseInt(
                    props.getProperty("cache.writeback.rate.limit", "100"));
            config.memoryWarningThreshold = Double.parseDouble(
                    props.getProperty("resource.memory.warning", "0.70"));
            config.memoryCriticalThreshold = Double.parseDouble(
                    props.getProperty("resource.memory.critical", "0.85"));
            config.cpuWarningThreshold = Double.parseDouble(
                    props.getProperty("resource.cpu.warning", "0.80"));
            config.kafkaBootstrapServers = props.getProperty(
                    "kafka.bootstrap.servers", "localhost:9092");
            config.kafkaInputTopic = props.getProperty(
                    "kafka.input.topic", "pipeline-raw-data");
            config.kafkaOutputTopic = props.getProperty(
                    "kafka.output.topic", "pipeline-cleansed-data");
            config.kafkaGroupId = props.getProperty(
                    "kafka.group.id", "cleansing-engine");

        } catch (Exception e) {
            System.err.println("Failed to load config from " + configPath
                    + ", using defaults. Error: " + e.getMessage());
        }
        return config;
    }

    // ---- Getters ----
    public long getMicroBatchIntervalMs() { return microBatchIntervalMs; }
    public int getWorkerParallelism() { return workerParallelism; }
    public long getTaskTimeoutMs() { return taskTimeoutMs; }
    public String getStorageRoot() { return storageRoot; }
    public long getTimeWindowMs() { return timeWindowMs; }
    public int getCacheDefaultTTL() { return cacheDefaultTTL; }
    public int getCacheMaxPoints() { return cacheMaxPoints; }
    public int getWriteBackQueueSize() { return writeBackQueueSize; }
    public int getWriteBackRateLimit() { return writeBackRateLimit; }
    public double getMemoryWarningThreshold() { return memoryWarningThreshold; }
    public double getMemoryCriticalThreshold() { return memoryCriticalThreshold; }
    public double getCpuWarningThreshold() { return cpuWarningThreshold; }
    public String getKafkaBootstrapServers() { return kafkaBootstrapServers; }
    public String getKafkaInputTopic() { return kafkaInputTopic; }
    public String getKafkaOutputTopic() { return kafkaOutputTopic; }
    public String getKafkaGroupId() { return kafkaGroupId; }

    @Override
    public String toString() {
        return "AppConfig{microBatch=" + microBatchIntervalMs + "ms"
                + ", parallelism=" + workerParallelism
                + ", cacheMaxPoints=" + cacheMaxPoints
                + ", storageRoot='" + storageRoot + "'"
                + ", kafka='" + kafkaBootstrapServers + "'}";
    }
}
