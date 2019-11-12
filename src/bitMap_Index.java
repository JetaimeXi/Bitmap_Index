import java.sql.*;

public class bitMap_Index {
    public static void main(String[] args) {
        Connection conn = null;
        Statement stmt = null;
        //导入驱动jar包
        // 注册驱动
        try {
            Class.forName("com.mysql.jdbc.Driver");
            // 获取数据库连接对象
            conn = DriverManager.getConnection(
                    "jdbc:mysql://localhost:3306/mysql",
                    "root",
                    "root");
            // 定义sql语句
            String sql = "select host, user from user";
            // 获取执行sql的对象Statement
            stmt = conn.createStatement();
            // 执行sql
            ResultSet rs = stmt.executeQuery(sql);
            // 处理结果
            while (rs.next()) {
                String host = rs.getString("host");
                String user = rs.getString("user");
                System.out.println(host + "---" + user);
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            // 释放资源
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

    }
}
