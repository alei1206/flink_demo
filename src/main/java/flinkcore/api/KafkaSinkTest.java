package flinkcore.api;

import flinkcore.utils.MyGeneratorFunction;
import flinkcore.utils.User;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.datagen.DataGenerator;

public class KafkaSinkTest {


    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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
                .setBootstrapServers("node1:9093")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("test")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        map.sinkTo(sink);
        env.execute();

    }






}
