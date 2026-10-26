package io.kestra.plugin.kafka;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProduceUnitTest {

    private final Produce task = Produce.builder().build();

    @Test
    void shouldProcessPartitionCorrectly() {
        assertThat(task.processPartition(null)).isNull();
        assertThat(task.processPartition(0)).isEqualTo(0);
        assertThat(task.processPartition(42)).isEqualTo(42);
        assertThat(task.processPartition(42L)).isEqualTo(42);
        assertThat(task.processPartition((short) 5)).isEqualTo(5);
        assertThat(task.processPartition("10")).isEqualTo(10);

        assertThatThrownBy(() -> task.processPartition(new Object()))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Invalid type of partition");
    }

    @Test
    void shouldProcessHeadersMapWithNullAndNonNullValues() {
        assertThat(task.processHeaders(null)).isNull();

        Map<String, Object> headers = new java.util.LinkedHashMap<>();
        headers.put("X-Custom", "test-val");
        headers.put("X-Marker", null);
        headers.put("X-Numeric", 123);

        Iterable<Header> result = task.processHeaders(headers);
        assertThat(result).isNotNull();

        List<Header> headerList = new ArrayList<>();
        result.forEach(headerList::add);

        assertThat(headerList).hasSize(3);
        assertThat(headerList.get(0).key()).isEqualTo("X-Custom");
        assertThat(headerList.get(0).value()).isEqualTo("test-val".getBytes(StandardCharsets.UTF_8));

        assertThat(headerList.get(1).key()).isEqualTo("X-Marker");
        assertThat(headerList.get(1).value()).isNull();

        assertThat(headerList.get(2).key()).isEqualTo("X-Numeric");
        assertThat(headerList.get(2).value()).isEqualTo("123".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void shouldProcessHeadersListSafely() {
        List<Map<String, String>> headersList = List.of(
            Map.of("header1", "val1"),
            Collections.emptyMap(),
            Map.of("header2", "val2")
        );

        Iterable<Header> result = task.processHeaders(headersList);
        assertThat(result).isNotNull();

        List<Header> list = new ArrayList<>();
        result.forEach(list::add);

        assertThat(list).hasSize(2);
        assertThat(list.get(0).key()).isEqualTo("header1");
        assertThat(list.get(0).value()).isEqualTo("val1".getBytes(StandardCharsets.UTF_8));
        assertThat(list.get(1).key()).isEqualTo("header2");
        assertThat(list.get(1).value()).isEqualTo("val2".getBytes(StandardCharsets.UTF_8));
    }
}
