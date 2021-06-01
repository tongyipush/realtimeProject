package com.atguigu.gmall.realtime.app.func;

import com.atguigu.gmall.realtime.utils.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.List;

/**
 *
 * 自定义UDTF函数，完成分词功能
 * 封装了分词工具类，但是flinksql无法直接使用分词工具类，因此需要封装一个函数
 * select ik_keyword(fullword) from xxxx;
 * SQL自定义函数：
 * UDF：1对1，例如转换大小写
 * UDTF：一条数据过来转换成多条数据
 * DAF：聚合函数，sum()、count()求和，求总值
 */

//注解表示输出这行类的类型
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class KeywordUDTF  extends TableFunction<Row> {

    //todo 为什么不使用重写方法
    // 因为自定义函数，不能限定死方法，需要设计者自己实现，不使用重写对自定义函数来说更加的灵活
    // 但是调用方便，方法的名字相同
    public void eval(String fullWord) {
        //todo 调用分词工具类对字符串进行分词
        List<String> keyWords = KeywordUtil.analyze(fullWord);
        for (String keyWord : keyWords) {
            collect(Row.of(keyWord));
        }
    }
}
