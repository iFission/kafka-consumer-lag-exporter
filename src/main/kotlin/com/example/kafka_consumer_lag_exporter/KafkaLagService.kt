package com.example.kafka_consumer_lag_exporter

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.common.TopicPartition
import org.springframework.kafka.core.KafkaAdmin
import org.springframework.stereotype.Service
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import kotlin.math.max

@Service
class KafkaLagService(
    private val kafkaAdmin: KafkaAdmin,
    private val meterRegistry: MeterRegistry
) {
    private val adminClient: AdminClient = AdminClient.create(kafkaAdmin.configurationProperties)

    private val lagMap = ConcurrentHashMap<String, Double>()

    fun exportLagMetrics() {
        val consumerGroups = getConsumerGroups()

      consumerGroups.forEach { consumerGroup ->
        val topicPartitions = getTopicPartitions(consumerGroup)
        topicPartitions.forEach { (tp, consumerOffset) ->
            val latestOffset = getLatestOffset(tp)
            val lag = max((latestOffset - consumerOffset), 0).toDouble()

            val key = "${consumerGroup}_${tp.topic()}_${tp.partition()}"

            lagMap[key] = lag

            meterRegistry.gauge(
                "kafka.consumergroup.lag",
                listOf(
                    Tag.of("consumergroup", consumerGroup),
                    Tag.of("topic", tp.topic()),
                    Tag.of("partition", tp.partition().toString())
                ),
                lagMap,
                { it[key] ?: 0.0 }
            )
          }
        }
    }

    private fun getConsumerGroups(): List<String> {
        return adminClient.listConsumerGroups().all()
            .get(10, TimeUnit.SECONDS)
            .map { it.groupId() }
    }

    private fun getTopicPartitions(consumerGroup: String): Map<TopicPartition, Long> {
        val offsets = adminClient.listConsumerGroupOffsets(consumerGroup).partitionsToOffsetAndMetadata()
            .get(10, TimeUnit.SECONDS)
        return offsets.mapValues { (_, offsetAndMetadata) -> offsetAndMetadata.offset() }
    }

    private fun getLatestOffset(topicPartition: TopicPartition): Long {
        val offsets = adminClient.listOffsets(
            mapOf(topicPartition to org.apache.kafka.clients.admin.OffsetSpec.latest())
        ).all().get()

        return offsets[topicPartition]?.offset() ?: 0L
    }
}

