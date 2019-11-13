import utils.JDBCUtils;

import java.sql.*;
import java.util.*;

public class bitMap_Index {
    private static Connection conn = null;
    private static PreparedStatement pstmt = null;
    private static ResultSet rs = null;
    private static ResultSetMetaData metaData = null;
    private static HashMap<String, BitSet> bitMap = new HashMap<>();    // 存放 key:列的取值 value:位向量
    private static HashMap<String, List<String>> lists = new HashMap<>(); // 存放 key:列名 value:列的取值
    private static String table = "custom_info";
    private static int count;

    /**
     * @Date: 2019/11/13 18:53
     * @Author: Tod
     * @Method: static initializer
     * @Parameters:
     * @param null
     * @Description: 静态代码块，当调用此类时执行，作用是获取每一列的取值并建立位图索引(位向量)
     * @Implementation: 先获取每列的列名，分组运算求每列的取值，遍历总的记录数获取每一列的取值的位向量，通过encode函数进行压缩位图
     * @Return: 无
     */
    static {
        try {
            // 调用utils.JDBCUtils工具类，获取数据库连接
            conn = JDBCUtils.getConnection();
            // 定义sql语句，查出记录总数
            String sql = "SELECT * FROM " + table;
            // 获取执行sql的PreparedStatement对象
            PreparedStatement pstmt = conn.prepareStatement(sql);
            ResultSet rs = pstmt.executeQuery();
            // 获取行数
            rs.last();
            count = rs.getRow();
            // 最后一点需要注意的是，无论是 ResultSet 还是 ResultSetMetaData，都是需要释放资源的
            metaData = rs.getMetaData();
            for (int i = 2; i <= metaData.getColumnCount(); i++) {
                // 获取列名称
                String columnName = metaData.getColumnName(i);
                // 获取每列的取值范围
                List<String> columnTypes = getColumnTypes(columnName);
                lists.put(columnName, columnTypes);
                // 初始化位图
                for (String columnType : columnTypes) {
                    bitMap.put(columnType, new BitSet());
                }
            }
//            System.out.println(lists);
//            System.out.println(bitMap);
            // 获取每个字段的位图值
            rs.first();
            // 遍历每行
            for (int j = 1; j <= count; j++) {
                // 遍历每列
                for (int i = 2; i <= metaData.getColumnCount(); i++) {
                    // 获取当前列的值，并置位图值
                    BitSet bitSet = bitMap.get(rs.getString(i));
                    bitSet.set(j);
                }
                // 指针移动
                rs.next();
            }
            System.out.println(bitMap);
//            Set<Map.Entry<String, BitSet>> entries = bitMap.entrySet();
//            for (Map.Entry<String, BitSet> entry : entries) {
//                // 获取值
//                BitSet bs = entry.getValue();
//                // 压缩位向量
//                BitSet ebs = encoding(bs);
//                // 设置值
//                entry.setValue(ebs);
//            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            JDBCUtils.close(rs, pstmt, conn);
        }
    }

    public static void main(String[] args) {
        System.out.println("hello");
        select(getSelectID(new BitSet[]{bitMap.get("F"), bitMap.get("L1")}));
        System.out.println(insert(new String[]{"Tod", "M", "Guangzhou", "L1"}));
        System.out.println(bitMap);
    }

    /**
     * @param groupBy 列名
     * @Date: 2019/11/13 3:29
     * @Author: Tod
     * @Method: getColumnTypes
     * @Parameters:
     * @Description: 通过分组查询语句获取每列的取值范围
     * @Implementation:
     * @Return: java.util.List<java.lang.String> 返回每列取值的范围
     */
    private static List<String> getColumnTypes(String groupBy) throws SQLException {
        List<String> list = new ArrayList<>();
        String groupby_sql = "SELECT " + groupBy + " FROM custom_info GROUP BY " + groupBy;
        // 获取执行sql的对象PreparedStatement
        pstmt = conn.prepareStatement(groupby_sql);
        // 执行sql语句
        rs = pstmt.executeQuery();
        // 处理结果
        while (rs.next()) {
            list.add(rs.getString(1));
        }
        return list;
    }

    /**
     * @param insertStr 插入记录对应的字符串集(不允许有空值，也可以有空值，但是为了数据完整性)
     * @Description: 插入记录及维护HashMap中的键值对
     * @Method: insert
     * @Implementation:
     * @Return: boolean 字符集未满足列的个数(除去id号)返回false，
     * @Date: 2019/11/13 23:57
     * @Author: Tod
     */
    private static boolean insert(String[] insertStr) {
        // 安全性检查(不允许有空值)
        try {
            if (insertStr.length < metaData.getColumnCount() - 1) {
                return false;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        // 记录数加1
        count++;
        // 执行insert操作
        // insert into 表名 values(值1,值2,...值n);
        StringBuffer sb = new StringBuffer("INSERT INTO custom_info VALUES(NULL, ");
        for (int i = 1; i < insertStr.length; i++) {
            sb.append("?, ");
        }
        sb.append("?)");
        if (executeSqlUpdate(sb.toString(), insertStr)) {
            // 判断插入的值是否在bitMap中
            for (String s : insertStr) {
                BitSet bitSet = bitMap.get(s);
                if (bitSet != null) {
                    // 是，则在其位置标1
                    bitSet.set(count);
                } else {
                    // 否，则在lists对应的列名中添加value，
                    // 在bitMap中插入一个键值对
                    bitSet = new BitSet();
                    bitSet.set(count);
                    bitMap.put(s, bitSet);
                }
            }
            return true;
        } else {
            return false;
        }
    }

    private static boolean delete(String deleteStr) {
        return false;
    }

    private static boolean update(String updateStr) {
        return false;
    }

    /**
     * @param selectCondition 传入多码查询对应的BitSet集合
     * @Description: 与运算，结果(记录号)存入List中返回
     * @Method: getSelectID
     * @Implementation:
     * @Return: java.util.List<java.lang.Integer>
     * @Date:
     * @Author: Tod
     */
    private static List<Integer> getSelectID(BitSet[] selectCondition) {
        BitSet temp = new BitSet();
        for (int i = selectCondition[0].nextSetBit(0); i >= 0; i = selectCondition[0].nextSetBit(i + 1)) {
            temp.set(i);
        }
        for (int i = 1; i < selectCondition.length; i++) {
            temp.and(selectCondition[i]);
        }
        List<Integer> list = new ArrayList<>();
        for (int i = temp.nextSetBit(0); i >= 0; i = temp.nextSetBit(i + 1)) {
            list.add(i);
        }
        return list;
    }

    /**
     * @param list 存放记录的id号
     * @Description: 实现对List存储的id号进行select操作
     * @Method: select
     * @Implementation:
     * @Return: void
     * @Date: 2019/11/13 22:48
     * @Author: Tod
     */
    private static void select(List<Integer> list) {
        try {
            // 获取数据库连接对象conn
            conn = JDBCUtils.getConnection();
            // 定义sql语句
            String sql = "SELECT * FROM custom_info WHERE id = ?";
            // 获取执行sql语句的PreparedStatement对象pstmt
            pstmt = conn.prepareStatement(sql);
            // 设置值
            for (Integer i : list) {
                pstmt.setInt(1, i);
                // 执行sql语句
                rs = pstmt.executeQuery();
                // 处理结果
                metaData = rs.getMetaData();
                for (int j = 1; j <= metaData.getColumnCount(); j++) {
                    System.out.print(metaData.getColumnName(j) + '\t' + '\t');
                }
                System.out.println();
                while (rs.next()) {
                    for (int j = 1; j <= metaData.getColumnCount(); j++) {
                        System.out.print(rs.getString(j) + '\t' + '\t');
                    }
                    System.out.println();
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            JDBCUtils.close(rs, pstmt, conn);
        }
    }
//    private static BitSet encoding(BitSet bitSet) {
//        int j = 0;
//        for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
////            System.out.print(i + " ");
//            int num = i - j - 1;
//            j = i;
//        }
//        System.out.println();
//        return bitSet;
//    }

    private static String decoding(String str) {
        return "";
    }

    /**
     * @param sql    构造对应带?符号的sql语句
     * @param params 对应?个数的参数列表
     * @Description: 执行INSERT、DELETE、UPDATE操作
     * @Method: executeSqlUpdate
     * @Implementation:
     * @Return: boolean 执行成功返回true
     * @Date: 2019/11/14 0:53
     * @Author: Tod
     */
    private static boolean executeSqlUpdate(String sql, String[] params) {
        try {
            // 获取数据库连接对象conn
            conn = JDBCUtils.getConnection();
            // 获取执行sql语句的PreparedStatement对象pstmt
            pstmt = conn.prepareStatement(sql);
            // 设置值
            for (int i = 1; i <= params.length; i++) {
                pstmt.setString(i, params[i - 1]);
            }
            // 执行sql语句
            pstmt.executeUpdate();
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        } finally {
            JDBCUtils.close(rs, pstmt, conn);
        }
    }
}
