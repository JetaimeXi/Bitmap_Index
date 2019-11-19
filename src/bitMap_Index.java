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
//                    bitSet.set(j);
                    bitSet.set(rs.getInt(1));
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
//        selectResult(getSelectID(new BitSet[]{bitMap.get("F"), bitMap.get("L1")}));
//        System.out.println(insert(new String[]{"Tod", "M", "Guangzhou", "L1"}));
//        System.out.println(bitMap);
//        System.out.println(update(new String[]{"name", "Cai", "gender", "F", "address", "Jieyang", "9"}));
        System.out.println(update(new String[]{"name", "Tod", "gender", "M", "address", "Guangzhou", "10"}));
////        System.out.println(delete(8));
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
        StringBuffer sb = new StringBuffer("INSERT INTO " + table + " VALUES(NULL, ");
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


    private static boolean delete(int id) {
//        DELETE FROM custom_info WHERE id=8;
        StringBuilder sb = new StringBuilder("DELETE FROM " + table + " WHERE id=?");
        if (executeSqlUpdate(sb.toString(), new String[]{String.valueOf(id)})) {
            for (BitSet value : bitMap.values()) {
                value.clear(id);
            }
            return true;
        } else {
            return false;
        }
    }

    private static boolean update(String[] updateStr) {
        // update 表名 set 列名1 = 值1, 列名2 = 值2,... [where 条件];
        // UPDATE custom_info SET NAME = "Jimmy" WHERE id = 7
        StringBuilder sb = new StringBuilder("UPDATE " + table + " SET ");
        String[] strings = new String[(updateStr.length >> 1) + 1];
        for (int i = 0; i < updateStr.length - 1; i = i + 2) {
            sb.append(updateStr[i] + "=?,");
            strings[i >> 1] = updateStr[i + 1];
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(" WHERE id=?");
        strings[strings.length - 1] = updateStr[updateStr.length - 1];
        int id = Integer.valueOf(updateStr[updateStr.length - 1]);
        // 修改之前的bitMap
        try {
            conn = JDBCUtils.getConnection();
            ResultSet rs = select(id);
            rs.next();
            for (int j = 2; j <= metaData.getColumnCount(); j++) {
                bitMap.get(rs.getString(j)).clear(id);
            }
            System.out.println(bitMap);
            if (executeSqlUpdate(sb.toString(), strings)) {
                // 判断插入的值是否在bitMap中
                conn = JDBCUtils.getConnection();
                rs = select(id);
                rs.next();
                for (int j = 2; j <= metaData.getColumnCount(); j++) {
                    BitSet bitSet = bitMap.get(rs.getString(j));
                    if (bitSet != null) {
                        // 是，则在其位置标1
                        bitSet.set(id);
                    } else {
                        // 否，则在lists对应的列名中添加value，
                        // 在bitMap中插入一个键值对
                        bitSet = new BitSet();
                        bitSet.set(id);
                        bitMap.put(rs.getString(j), bitSet);
                    }
                }
                return true;
            } else {
                return false;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        } finally {
            JDBCUtils.close(rs, pstmt, conn);
        }
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

    private static void selectResult(List<Integer> list) {
        // 获取数据库连接对象conn
        try {
            conn = JDBCUtils.getConnection();
            boolean flag = true;
            for (Integer integer : list) {
                ResultSet rs = select(integer.intValue());
                if (flag) {
                    // 打印表头
                    metaData = rs.getMetaData();
                    for (int j = 1; j <= metaData.getColumnCount(); j++) {
                        System.out.print(metaData.getColumnName(j) + '\t' + '\t');
                    }
                    System.out.println();
                    flag = false;
                }
                print(rs);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            JDBCUtils.close(rs, pstmt, conn);
        }
    }

    /**
     * @param id 存放记录的id号
     * @Description: 实现对id号进行select操作
     * @Method: select
     * @Implementation:
     * @Return: void
     * @Date: 2019/11/13 22:48
     * @Author: Tod
     */
    private static ResultSet select(int id) throws SQLException {
        // 定义sql语句
        String sql = "SELECT * FROM custom_info WHERE id = ?";
        // 获取执行sql语句的PreparedStatement对象pstmt
        pstmt = conn.prepareStatement(sql);
        // 设置值
        pstmt.setInt(1, id);
        // 执行sql语句
        rs = pstmt.executeQuery();
        return rs;
    }

    private static void print(ResultSet rs) throws SQLException {
        while (rs.next()) {
            for (int j = 1; j <= metaData.getColumnCount(); j++) {
                System.out.print(rs.getString(j) + '\t' + '\t');
            }
            System.out.println();
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
            for (int i = 0; i < params.length; i++) {
                if (i == params.length - 1 && (params[i].charAt(0) - '0' >= 0) && (params[i].charAt(0) - '0' <= 9)) {
                    Integer integer = Integer.valueOf(params[i]);
                    pstmt.setInt(i + 1, integer.intValue());
                    continue;
                }
                pstmt.setString(i + 1, params[i]);
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
