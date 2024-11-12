package com.example.kafka_consumer_lag_exporter

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.springframework.kafka.core.KafkaAdmin
import org.springframework.stereotype.Service
import java.util.concurrent.TimeUnit

@Service
class KafkaLagService(
    private val kafkaAdmin: KafkaAdmin,
    private val meterRegistry: MeterRegistry
) {
    private val adminClient: AdminClient = AdminClient.create(kafkaAdmin.configurationProperties)

    fun exportLagMetrics() {
        val consumerGroup = "group1"
        val topicPartitions = getTopicPartitions(consumerGroup)

        topicPartitions.forEach { (tp, consumerOffset) ->
            val latestOffset = getLatestOffset(tp)
            val lag = latestOffset - consumerOffset
            meterRegistry.gauge(
                "kafka.consumer.lag",
                listOf(
                    Tag.of("topic", tp.topic()),
                    Tag.of("partition", tp.partition().toString())
                ),
                lag.toInt()
            )
        }
    }

    private fun getTopicPartitions(consumerGroup: String): Map<TopicPartition, Long> {
        val offsets = adminClient.listConsumerGroupOffsets(consumerGroup).partitionsToOffsetAndMetadata()
            .get(10, TimeUnit.SECONDS)
        return offsets.mapValues { (_, offsetAndMetadata) -> offsetAndMetadata.offset() }.also(::println)
    }

    private fun getLatestOffset(topicPartition: TopicPartition): Long {
        val offsets = adminClient.listOffsets(
            mapOf(topicPartition to org.apache.kafka.clients.admin.OffsetSpec.latest())
        ).all().get()

        return offsets[topicPartition]?.offset() ?: 0L
    }
}

