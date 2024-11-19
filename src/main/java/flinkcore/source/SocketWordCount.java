package flinkcore.source;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.awt.peer.CanvasPeer;

public class SocketWordCount {

    public static void main(String[] args) throws Exception {



        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ExecutionConfig config = env.getConfig();

        //设置这个环境的并行度，此指会覆盖环境的默认并行度。
        //环境的默认并行度与本地环境的cpu线程数有关
        config.setParallelism(2);



        //基于socker
        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);
        //基于集合，算是批处理，有界流
//        DataStreamSource<String> source = env.fromElements("flink", "hadoop", "flink");

        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMap = source.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {

                for (String s : value.split(" ")) {
                    out.collect(Tuple2.of(s, 1));
                }
            }
        });

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = flatMap.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> reduce = keyedStream.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {

                return Tuple2.of(value1.f0, value1.f1 + value2.f1);

            }
        });

        reduce.print();

        env.execute();




    }


}
