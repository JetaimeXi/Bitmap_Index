import utils.JDBCUtils;

import java.sql.*;
import java.util.*;

public class bitMap_Index {
    private static Connection conn = null;
    private static PreparedStatement pstmt = null;
    private static ResultSet rs = null;
    private static HashMap<String, BitSet> bitMap = new HashMap<>();
    private static HashMap<String, List<String>> lists = new HashMap<>();
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
            ResultSetMetaData metaData = rs.getMetaData();
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
        System.out.println(getSelectID(new BitSet[]{bitMap.get("F"), bitMap.get("L1")}));
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

    private static boolean insert(String insertStr) {
        return false;
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

        BitSet temp = selectCondition[0];
        for (int i = 1; i < selectCondition.length; i++) {
            temp.and(selectCondition[i]);
        }
        List<Integer> list = new ArrayList<>();
        for (int i = temp.nextSetBit(0); i >= 0; i = temp.nextSetBit(i + 1)) {
            list.add(i);
        }
        return list;
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


}
