package com.example.kafka_consumer_lag_exporter

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.scheduling.annotation.EnableScheduling

@EnableScheduling
@SpringBootApplication
class KafkaConsumerLagExporterApplication

fun main(args: Array<String>) {
    runApplication<KafkaConsumerLagExporterApplication>(*args)
}
