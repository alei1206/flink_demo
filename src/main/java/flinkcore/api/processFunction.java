package flinkcore.api;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class processFunction {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stream1 = env.fromElements("1", "2", "3","2");

        KeyedStream<String, String> keyedStream = stream1.keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String s) throws Exception {
                return s;
            }
        });
        keyedStream.process(new KeyedProcessFunction<String, String, Object>() {
            @Override
            public void processElement(String value, KeyedProcessFunction<String, String, Object>.Context ctx, Collector<Object> out) throws Exception {

            }
        })

        SingleOutputStreamOperator<Tuple2<String, Long>> counts = stream1.process(new ProcessFunction<String, Tuple2<String, Long>>() {

            @Override
            public void open()

            @Override
            public void processElement(String word, ProcessFunction<String, Tuple2<String, Long>>.Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
                out.collect(new Tuple2<>(word,  1L));
            }
        });

        counts.print();

        env.execute();

    }


}
