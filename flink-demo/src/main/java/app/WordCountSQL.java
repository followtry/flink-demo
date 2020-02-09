package app;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.LocalEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

/**
 * 在本地执行批量数据的sql 查询
 * @author jingzhongzhi
 * @since 2020/2/9
 */
public class WordCountSQL {
    
    /**
     * main.
     */
    public static void main(String[] args) throws Exception{
        LocalEnvironment env = ExecutionEnvironment.createLocalEnvironment();
    
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);
    
        DataSet<WC> wcDataSource = env.fromElements(
                new WC("hello", 1),
                new WC("jing", 2),
                new WC("zhong", 3),
                new WC("zhong", 3),
                new WC("zhong", 3),
                new WC("zhong", 3),
                new WC("zhong", 3),
                new WC("zhi", 3),
                new WC("teng", 1),
                new WC("teng", 1)
        );
    
        //注册表名和数据集
        tEnv.registerDataSet("WordCount",wcDataSource);
        //用标准 sql查询数据
        Table table = tEnv.sqlQuery("SELECT word, SUM(frequency) as frequency FROM WordCount GROUP BY word order by word desc");
    
        DataSet<WC> dataSet = tEnv.toDataSet(table, WC.class);
        dataSet.print();
    }
    
    public static class WC {
        public String word;
        public long frequency;
        
        // public constructor to make it a Flink POJO
        public WC() {}
        
        public WC(String word, long frequency) {
            this.word = word;
            this.frequency = frequency;
        }
        
        @Override
        public String toString() {
            return "WC " + word + " " + frequency;
        }
    }
    
    
}
