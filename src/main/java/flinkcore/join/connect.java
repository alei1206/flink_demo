package flinkcore.join;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class connect {

    //connect连接并不是得到一个stream，而是得到一个ConnectedStreams连接流。连接流本质上是两个流放到一个流中，内部各保持数据形势不变，彼此之间独立。
    //因此connect之后，需要使用CoMapFunction函数进行同处理，将两个流转换为一个流。
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> stream1 = env.fromElements(1, 2, 3);
        DataStreamSource<Long> stream2 = env.fromElements(1L, 2L, 3L);


        //连接两个流
        ConnectedStreams<Integer, Long> connected = stream1.connect(stream2);

       //第一个参数是第一个流的，第二个参数是第二个流的，第三个类型是输出流的
        SingleOutputStreamOperator<String> stream3 = connected.map(new CoMapFunction<Integer, Long, String>() {
            @Override
            public String map1(Integer integer) throws Exception {
                return "stream1: "+ integer;
            }
            @Override
            public String map2(Long aLong) throws Exception {
                return "stream2: "+ aLong;
            }
        });
        stream3.print();
        env.execute();
    }
}
