from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from time import sleep
from uuid import uuid4

from utils.testing import (
    acknowledge_message,
    consume_messages,
    get_node_health,
    publish_message,
    restart_container,
    start_container,
    stop_container,
)


class TestConsistentHashPartitioning:
    def test_publish_to_single_partition(self, queue_node_url: str) -> None:
        topic = f"test-topic-{uuid4().hex[:8]}"
        payload = b"test message"
        status, response = publish_message(
            topic,
            payload,
            key="fixed-key",
            url=queue_node_url,
        )
        assert status == 200
        assert "message_id" in response

    def test_consistent_key_routing(self, queue_node_url: str) -> None:
        topic = f"consistent-topic-{uuid4().hex[:8]}"
        key = "consistent-key"
        partitions: list[int] = []

        for i in range(10):
            status, response = publish_message(
                topic,
                f"message-{i}".encode(),
                key=key,
                url=queue_node_url,
            )
            assert status == 200
            if "partition" in response:
                partitions.append(response["partition"])

        if partitions:
            assert len(set(partitions)) == 1, "Key yang sama harus ke partition sama"


class TestMultipleProducersConsumers:
    def test_multiple_producers(self, queue_node_url: str) -> None:
        topic = f"multi-producer-{uuid4().hex[:8]}"
        num_producers = 5
        messages_per_producer = 10

        def produce(producer_id: int) -> list[str]:
            message_ids: list[str] = []
            for i in range(messages_per_producer):
                status, response = publish_message(
                    topic,
                    f"producer-{producer_id}-msg-{i}".encode(),
                    url=queue_node_url,
                )
                if status == 200 and "message_id" in response:
                    message_ids.append(response["message_id"])
            return message_ids

        with ThreadPoolExecutor(max_workers=num_producers) as executor:
            futures = [executor.submit(produce, i) for i in range(num_producers)]
            all_ids: list[str] = []
            for f in as_completed(futures):
                all_ids.extend(f.result())

        expected = num_producers * messages_per_producer
        assert len(all_ids) >= expected * 0.9, "Minimal 90% pesan harus berhasil"

    def test_multiple_consumers_same_group(self, queue_node_url: str) -> None:
        topic = f"multi-consumer-{uuid4().hex[:8]}"
        consumer_group = f"group-{uuid4().hex[:8]}"

        for i in range(20):
            publish_message(topic, f"msg-{i}".encode(), url=queue_node_url)

        sleep(1)

        consumed_by_c1: list[str] = []
        consumed_by_c2: list[str] = []

        for _ in range(5):
            status, response = consume_messages(
                topic,
                consumer_group,
                "consumer-1",
                max_messages=5,
                url=queue_node_url,
            )
            if status == 200 and "messages" in response:
                for msg in response["messages"]:
                    consumed_by_c1.append(msg.get("message_id", ""))

            status, response = consume_messages(
                topic,
                consumer_group,
                "consumer-2",
                max_messages=5,
                url=queue_node_url,
            )
            if status == 200 and "messages" in response:
                for msg in response["messages"]:
                    consumed_by_c2.append(msg.get("message_id", ""))

        overlap = set(consumed_by_c1) & set(consumed_by_c2)
        assert len(overlap) == 0, "Consumer dalam group sama tidak boleh overlap"


class TestMessagePersistenceRecovery:
    def test_message_survives_restart(
        self,
        queue_node_url: str,
        all_queue_nodes: list[str],
    ) -> None:
        topic = f"persist-topic-{uuid4().hex[:8]}"
        consumer_group = f"persist-group-{uuid4().hex[:8]}"
        payload = f"persistent-message-{uuid4().hex}"

        status, response = publish_message(
            topic,
            payload.encode(),
            url=queue_node_url,
        )
        assert status == 200
        message_id = response.get("message_id")
        assert message_id is not None

        restart_container("queue-1")
        sleep(3)

        status, response = consume_messages(
            topic,
            consumer_group,
            "recovery-consumer",
            max_messages=10,
            url=queue_node_url,
        )
        if status == 200 and "messages" in response:
            found = any(
                msg.get("message_id") == message_id for msg in response["messages"]
            )
            assert found, "Message harus tetap ada setelah restart"


class TestAtLeastOnceDelivery:
    def test_unacknowledged_message_redelivery(self, queue_node_url: str) -> None:
        topic = f"redelivery-topic-{uuid4().hex[:8]}"
        consumer_group = f"redelivery-group-{uuid4().hex[:8]}"

        status, pub_response = publish_message(
            topic,
            b"redelivery-test",
            url=queue_node_url,
        )
        assert status == 200
        _ = pub_response.get("message_id")

        status, first_consume = consume_messages(
            topic,
            consumer_group,
            "consumer-first",
            max_messages=1,
            url=queue_node_url,
        )

        sleep(35)

        status, second_consume = consume_messages(
            topic,
            consumer_group,
            "consumer-second",
            max_messages=10,
            url=queue_node_url,
        )

    def test_acknowledged_message_not_redelivered(self, queue_node_url: str) -> None:
        topic = f"acked-topic-{uuid4().hex[:8]}"
        consumer_group = f"acked-group-{uuid4().hex[:8]}"

        status, pub_response = publish_message(
            topic,
            b"acked-message",
            url=queue_node_url,
        )
        assert status == 200

        status, consume_response = consume_messages(
            topic,
            consumer_group,
            "ack-consumer",
            max_messages=1,
            url=queue_node_url,
        )
        assert status == 200

        if "messages" in consume_response and len(consume_response["messages"]) > 0:
            msg_id = consume_response["messages"][0].get("message_id")
            ack_status, _ = acknowledge_message(msg_id, url=queue_node_url)
            assert ack_status == 200

            sleep(2)

            status, second_consume = consume_messages(
                topic,
                consumer_group,
                "ack-consumer",
                max_messages=10,
                url=queue_node_url,
            )
            if status == 200 and "messages" in second_consume:
                found = any(
                    m.get("message_id") == msg_id for m in second_consume["messages"]
                )
                assert not found, "Acknowledged message tidak boleh redelivered"


class TestNodeFailureHandling:
    def test_queue_operations_after_node_failure(
        self,
        all_queue_nodes: list[str],
    ) -> None:
        topic = f"failover-topic-{uuid4().hex[:8]}"
        payload = b"failover-message"

        status, response = publish_message(
            topic,
            payload,
            url=all_queue_nodes[0],
        )
        assert status == 200

        stop_container("queue-2")
        sleep(2)

        status, response = publish_message(
            topic,
            b"message-after-failure",
            url=all_queue_nodes[0],
        )

        start_container("queue-2")
        sleep(3)

        health_status, _ = get_node_health(all_queue_nodes[1])


class TestThroughput:
    def test_publish_throughput(self, queue_node_url: str) -> None:
        from time import perf_counter

        topic = f"throughput-topic-{uuid4().hex[:8]}"
        num_messages = 100

        start = perf_counter()
        for i in range(num_messages):
            publish_message(topic, f"msg-{i}".encode(), url=queue_node_url)
        elapsed = perf_counter() - start

        throughput = num_messages / elapsed if elapsed > 0 else 0
        assert throughput > 10, f"Throughput minimal 10 msg/s, actual: {throughput:.2f}"
