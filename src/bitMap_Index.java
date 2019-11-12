import utils.JDBCUtils;

import java.sql.*;

public class bitMap_Index {
    public static void main(String[] args) {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            // 调用utils.JDBCUtils工具类，获取数据库连接
            conn = JDBCUtils.getConnection();
            // 获取执行sql的对象Statement
            stmt = conn.createStatement();
            // 定义sql语句
            String sql = "select * from custom_info";
            // 执行sql语句
            rs = stmt.executeQuery(sql);
            // 处理结果
            while (rs.next()) {
                System.out.println(
                        rs.getString("id") + "--" +
                                rs.getString("name")+ "--" +
                                rs.getString("gender")+ "--" +
                                rs.getString("Address")+ "--" +
                                rs.getString("I_Level")
                );
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            JDBCUtils.close(rs, stmt, conn);
        }
    }
}
