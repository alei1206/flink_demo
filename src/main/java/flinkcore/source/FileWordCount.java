package flinkcore.source;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.util.Collector;

public class FileWordCount {


    public static void main(String[] args) throws Exception {



        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ExecutionConfig config = env.getConfig();

        String path = "file:///D:/BigData/code/Flink/flink-demo/src/main/resources/wordcount.txt";

        //PROCESS_ONCE 模式是读取文件，读取完成后，程序退出。
        //PROCESS_CONTINUOUSLY 模式是一直监听指定的文件或目录，2 秒钟检测一次文件是否发生变化。
        DataStreamSource<String> source = env.readFile(new TextInputFormat(null), path,
                FileProcessingMode.PROCESS_CONTINUOUSLY, 2000);

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
