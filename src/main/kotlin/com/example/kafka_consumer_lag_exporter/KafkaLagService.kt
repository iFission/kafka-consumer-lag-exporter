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
            val partitionsCommittedOffsets = getTopicPartitionsCommittedOffsets(consumerGroup)
            val topicPartitions = partitionsCommittedOffsets.keys
            val partitionsLatestOffsets = getTopicPartitionsLatestOffsets(topicPartitions)

            topicPartitions.forEach { topicPartition ->
                val committedOffset = partitionsCommittedOffsets[topicPartition] ?: 0L
                val latestOffset = partitionsLatestOffsets[topicPartition] ?: 0L
                val lag = max((latestOffset - committedOffset), 0).toDouble()

                val key = "${consumerGroup}_${topicPartition.topic()}_${topicPartition.partition()}"

                lagMap[key] = lag

                meterRegistry.gauge(
                    "kafka.consumergroup.lag",
                    listOf(
                        Tag.of("consumergroup", consumerGroup),
                        Tag.of("topic", topicPartition.topic()),
                        Tag.of("partition", topicPartition.partition().toString())
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

    private fun getTopicPartitionsCommittedOffsets(consumerGroup: String): Map<TopicPartition, Long> {
        val offsets = adminClient.listConsumerGroupOffsets(consumerGroup).partitionsToOffsetAndMetadata()
            .get(10, TimeUnit.SECONDS)
        return offsets.mapValues { (_, offsetAndMetadata) -> offsetAndMetadata.offset() }
    }

    private fun getTopicPartitionsLatestOffsets(topicPartitions: Set<TopicPartition>): Map<TopicPartition, Long> {
        return adminClient.listOffsets(
            topicPartitions.associateWith { org.apache.kafka.clients.admin.OffsetSpec.latest() }
        ).all().get(10, TimeUnit.SECONDS).mapValues { (_, offsetResult) -> offsetResult.offset() }

    }
}

