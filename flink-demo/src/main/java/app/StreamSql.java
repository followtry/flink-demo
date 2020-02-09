package app;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.util.Arrays;
import java.util.Objects;

/**
 * 流式表的两个表关联的实例
 * @author jingzhongzhi
 * @since 2020/2/9
 */
public class StreamSql {
    
    /**
     * main.
     */
    public static void main(String[] args) throws Exception{
        final ParameterTool param = ParameterTool.fromArgs(args);
        String planner = param.has("planner") ? param.get("planner") : "flink";
        
        LocalStreamEnvironment localEnv = StreamExecutionEnvironment.createLocalEnvironment();
        StreamTableEnvironment tEnv;
        if (Objects.equals(planner, "blink")) {    // use blink planner in streaming mode
            EnvironmentSettings settings = EnvironmentSettings.newInstance()
                    .useBlinkPlanner()
                    .inStreamingMode()
                    .build();
            tEnv = StreamTableEnvironment.create(localEnv, settings);
        } else if (Objects.equals(planner, "flink")) {    // use flink planner in streaming mode
            tEnv = StreamTableEnvironment.create(localEnv);
        } else {
            System.err.println("The planner is incorrect. Please run 'StreamSQLExample --planner <planner>', " +
                    "where planner (it is either flink or blink, and the default is flink) indicates whether the " +
                    "example uses flink planner or blink planner.");
            return;
        }
        
        DataStream<Order> orderA = localEnv.fromCollection(Arrays.asList(
                new Order(1001L, "apple", 3),
                new Order(1001L, "orange", 4),
                new Order(1003L, "apple", 3)
        ));
        
        DataStream<Order> orderB = localEnv.fromCollection(Arrays.asList(
                new Order(1004L, "pen", 3),
                new Order(1005L, "orange", 2),
                new Order(1005L, "apple", 8)
        ));
        
        Table tableA = tEnv.fromDataStream(orderA);
        tEnv.registerDataStream("OrderB", orderA);
        
        
        Table table = tEnv.sqlQuery("SELECT * FROM " + tableA + " WHERE amount > 0 UNION ALL " +
                "SELECT * FROM OrderB WHERE amount < 20");
        
        tEnv.toAppendStream(table, Order.class).print();
        
        localEnv.execute("stream-sql-plan");
    }
    
    public static class Order {
        public Long user;
        public String product;
        public int amount;
        
        public Order() {
        }
        
        public Order(Long user, String product, int amount) {
            this.user = user;
            this.product = product;
            this.amount = amount;
        }
        
        @Override
        public String toString() {
            return "Order{" +
                    "user=" + user +
                    ", product='" + product + '\'' +
                    ", amount=" + amount +
                    '}';
        }
    }
}
