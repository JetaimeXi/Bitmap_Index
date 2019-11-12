package utils;

import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.sql.*;
import java.util.Properties;

/**
 * @Author: Tod
 * @Description: JDBC工具类
 * @Date: Created in 2019/11/12 16:02
 * @Version: 1.0
 */
public class JDBCUtils {
    private static String url;
    private static String user;
    private static String password;
    private static String driver;

    /**
     * @Date: 2019/11/12 19:57
     * @Author: Tod
     * @Method: static initializer
     * @Parameters:
     * @param null
     * @Description: 静态代码块，初始化操作，在jdbc.properties文件中配置数据库
     * @Implementation:
     * @Return:
     */
    static {
        try {
            // 创建Properties集合类
            Properties pro = new Properties();
            // 获取src路径
            ClassLoader classLoader = JDBCUtils.class.getClassLoader(); //以src为根路径
            // 获取utils文件夹下的jdbc.properties文件
            URL res = classLoader.getResource("utils/jdbc.properties");
            String path = res.getPath(); //获取其路径
            // 加载经properties集合类
            pro.load(new FileReader(path));
            // 获取数据
            url = pro.getProperty("url");
            user = pro.getProperty("user");
            password = pro.getProperty("password");
            driver = pro.getProperty("driver");
            // 注册驱动
            Class.forName(driver);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }


    /**
     * @param
     * @Date: 2019/11/12 19:59
     * @Author: Tod
     * @Method: getConnection
     * @Parameters:
     * @Description: 获取连接
     * @Implementation:
     * @Return: java.sql.Connection 连接对象
     */
    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(url, user, password);
    }

    /**
     * @param stmt
     * @param conn
     * @Date: 2019/11/12 20:06
     * @Author: Tod
     * @Method: close
     * @Parameters:
     * @Description: 关闭连接，释放资源
     * @Implementation: 对应于stmt.executeUpdate操作
     * @Return: void
     */
    public static void close(Statement stmt, Connection conn) {
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

    /**
     * @param rs
     * @param stmt
     * @param conn
     * @Date: 2019/11/12 20:09
     * @Author: Tod
     * @Method: close
     * @Parameters:
     * @Description: 关闭连接，释放资源
     * @Implementation: 对应于stmt.executeQuery操作
     * @Return: void
     */
    public static void close(ResultSet rs, Statement stmt, Connection conn) {
        // 释放资源
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
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
