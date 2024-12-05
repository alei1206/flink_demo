package api;

import flinkcore.utils.MyGeneratorFunction;
import flinkcore.utils.User;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

public class DatagenSourceTest {

    @Test
    public void test1() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        GeneratorFunction<Long, User> generatorFunction = new MyGeneratorFunction();  //数据生成规则
        long numberOfRecords = Long.MAX_VALUE;   //数据生成总数
        RateLimiterStrategy rateLimiterStrategy = RateLimiterStrategy.perSecond(5); //生成速率


        DataGeneratorSource<User> source = new DataGeneratorSource<>(generatorFunction, numberOfRecords, rateLimiterStrategy, TypeInformation.of(User.class));

        DataStreamSource<User> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Generator Source");

        stream.print();
        env.execute();

    }


}
