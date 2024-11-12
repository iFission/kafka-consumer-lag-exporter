package com.example.kafka_consumer_lag_exporter

import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

@Component
class LagExporterScheduler(private val kafkaLagService: KafkaLagService) {

    @Scheduled(fixedRate = 1000)
    fun exportMetrics() {
        kafkaLagService.exportLagMetrics()
    }
}
