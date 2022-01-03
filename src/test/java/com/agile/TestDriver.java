package com.agile;

import cloud.agileframework.sql.SqlUtil;
import com.alibaba.druid.DbType;
import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.google.common.collect.Maps;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class TestDriver {
    public static int execut(Connection connection, String sql, Object param) {
        try (
                Statement statement = connection.createStatement();
        ) {
            return statement.executeUpdate(SqlUtil.parserSQLByType(DbType.elastic_search,sql,param));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    @Test
    public void test() throws SQLException, IllegalAccessException {
        DruidDataSource dataSource = new DruidDataSource();
        dataSource.setUrl("jdbc:elastic://localhost:9200?useSSL=true&trustSelfSigned=true");
        dataSource.setDriverClassName("cloud.agileframework.elasticsearch.Driver"); //这个可以缺省的，会根据url自动识别
        dataSource.setUsername("admin");
        dataSource.setPassword("admin");
        //下面都是可选的配置
        dataSource.setInitialSize(10);  //初始连接数，默认0
        dataSource.setMaxActive(30);  //最大连接数，默认8
        dataSource.setMinIdle(10);  //最小闲置数
        dataSource.setMaxWait(2000);  //获取连接的最大等待时间，单位毫秒
        dataSource.setPoolPreparedStatements(true); //缓存PreparedStatement，默认false
        dataSource.setMaxOpenPreparedStatements(20); //缓存PreparedStatement的最大数量，默认-1（不缓存）。大于0时会自动开启缓存PreparedStatement，所以可以省略上一句代码
        DruidPooledConnection con = dataSource.getConnection();
        int a = execut(con, "CREATE TABLE Persons6\n" +
                "(\n" +
                "Id_P integer,\n" +
                "LastName text,\n" +
                "FirstName keyword,\n" +
                "Address double,\n" +
                "City keyword comment '\\{\"copy_to\":\"LastName\"\\}'\n" +
                ")  comment '\\{\n" +
                "  \"settings\": \\{\n" +
                "    \"number_of_shards\": 12,\n" +
                "    \"number_of_replicas\": 0\n" +
                "  \\}\n" +
                "\\}'", Maps.newHashMap());
//
////        
//
//        IntStream.range(0,100).forEach(i->{
//            int b = execut(con, "INSERT INTO Persons6 (name,age,sex) VALUES ('tongmeng',"+i+",true)", Maps.newHashMap());
//
//        });
//
        int c = execut(con, "UPDATE persons6 SET name = 'tongmeng' WHERE name = '\\{name\\}'", Maps.newHashMap());

//        int d = execut(con,"delete from persons6 WHERE sex is null", Maps.newHashMap());
        System.out.println(c);
        con.close();
    }
}
