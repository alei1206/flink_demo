package api;

import flinkcore.utils.MyGeneratorFunction;
import flinkcore.utils.User;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.apache.flink.configuration.Configuration;

import javax.lang.model.element.ElementVisitor;

public class KafkaTest {

    @Test
    @DisplayName("向kafka中写入字符串")
    public void test1() throws Exception {

        Configuration configuration = new Configuration();
        configuration.setString("rest.port","9091");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.setParallelism(1);

        org.apache.flink.connector.datagen.source.DataGeneratorSource<User> source = new DataGeneratorSource<>(
                new MyGeneratorFunction(), Long.MAX_VALUE, RateLimiterStrategy.perSecond(1), TypeInformation.of(User.class));

        DataStreamSource<User> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Generator Source");

        SingleOutputStreamOperator<String> map = stream.map(new MapFunction<User, String>() {
            @Override
            public String map(User value) throws Exception {
                return value.getName();
            }
        });

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("192.168.217.130:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("test")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .build();
        map.sinkTo(sink);
        env.execute();
    }


    @Test
    @DisplayName("不同容错语义")
    public void test2() throws Exception {


    }

    @Test
    @DisplayName("avro序列化器")
    public void test3() throws Exception {


    }


    @Test
    @DisplayName("kafka source test")
    public void test_source() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("node1:9092")
                .setTopics("test")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> kafkaSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        kafkaSource.print();

        env.execute();

    }


}
