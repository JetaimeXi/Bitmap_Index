import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class bitMap_Index {
    public static void main(String[] args) throws Exception {
        //导入驱动jar包
        // 注册驱动
        Class.forName("com.mysql.jdbc.Driver");
        // 获取数据库连接对象
        Connection conn = DriverManager.getConnection(
                "jdbc:mysql://localhost:3306/mysql",
                "root",
                "root");
        // 定义sql语句
        String sql = "select host, user from user";
        // 获取执行sql的对象Statement
        Statement stmt = conn.createStatement();
        // 执行sql
        ResultSet rs = stmt.executeQuery(sql);
        // 处理结果
        while (rs.next()) {
            String host = rs.getString("host");
            String user = rs.getString("user");
            System.out.println(host+"---"+user);
        }
        // 释放资源
        stmt.close();
        conn.close();
    }
}
