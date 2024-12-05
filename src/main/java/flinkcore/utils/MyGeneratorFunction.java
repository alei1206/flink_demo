package flinkcore.utils;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.datagen.source.GeneratorFunction;


/**
 * GeneratorFunction<T, O>接口说明：
 *     功能说明：
 *         数据生成器函数的基本接口，用来定义生成数据的核心逻辑
 *     泛型说明：
 *         T ： 输入数据的类型(默认为Long)，表示数据生成器的自增ID
 *         O ： 输出数据的类型，要指定flink的数据类型(TypeInformation)
 *     实现方法：
 *         open   ： 创建对象时，调用一次，用来做资源初始化
 *         close  ： 销毁对象时，调用一次，用来做资源关闭
 *         map    ： 数据的生成逻辑，每生成一次数据调用一次，参数为自增ID
 */
public class MyGeneratorFunction implements GeneratorFunction<Long, User> {

    // 定义随机数数据生成器
    public RandomDataGenerator generator;

    // 初始化随机数数据生成器
    @Override
    public void open(SourceReaderContext readerContext) throws Exception {
        generator = new RandomDataGenerator();
    }

    @Override
    public User map(Long value) throws Exception {
        // 使用 随机数数据生成器 来创建 FlinkUser实例
        User user = new User(
                generator.nextHexString(4), //生成4位随机的字符串
                generator.nextInt(1, 100), // 生成随机数字
                System.currentTimeMillis()
        );
        return user;
    }

}
