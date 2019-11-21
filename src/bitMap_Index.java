import utils.JDBCUtils;

import java.sql.*;
import java.util.*;

public class bitMap_Index {
    private static Connection conn = null;
    private static PreparedStatement pstmt = null;
    private static ResultSet rs = null;
    private static ResultSetMetaData metaData = null;
    private static HashMap<String, BitSet> bitMap = new HashMap<>();    // 存放 key:列的取值 value:位向量
//    private static HashMap<String, List<String>> lists = new HashMap<>(); // 存放 key:列名 value:列的取值     同样没有作用
    private static HashMap<String, Integer> length = new HashMap<>();   // 存放 key:列的取值 value:压缩位图后的长度 用途：解压缩
    private static String table = "custom_info";
//    private static int count;   //并没有作用

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
            rs = pstmt.executeQuery();
            metaData = rs.getMetaData();
            // 获取列数
            int columnCount = metaData.getColumnCount();
            // 获取行数
            rs.last();
            int rowCount = rs.getRow();
            // 最后一点需要注意的是，无论是 ResultSet 还是 ResultSetMetaData，都是需要释放资源的
            for (int i = 2; i <= columnCount; i++) {
                // 获取列名称
                String columnName = metaData.getColumnName(i);
                // 获取每列的取值范围
                List<String> columnTypes = getColumnTypes(columnName);   //注意
//                lists.put(columnName, columnTypes);
                // 初始化位图
                for (String columnType : columnTypes) {
                    bitMap.put(columnType, new BitSet());
                }
            }
//            System.out.println(bitMap);
//            // 获取每个字段的位图值
            // 遍历每行
            rs = pstmt.executeQuery(); // 再执行一次
            rs.next();
            for (int j = 1; j <= rowCount; j++) {
                // 遍历每列
                for (int i = 2; i <= columnCount; i++) {
                    // 获取当前列的值，并置位图值
                    BitSet bitSet = bitMap.get(rs.getString(i));
                    // 设置id为其位图索引标志
                    bitSet.set(rs.getInt(1));
                }
                // 指针移动
                rs.next();
            }
            System.out.println("static压缩前:" + bitMap);
            // 压缩位图并设置压缩位图的长度
            for (Map.Entry<String, BitSet> stringBitSetEntry : bitMap.entrySet()) {
                length.put(stringBitSetEntry.getKey(), encoding(stringBitSetEntry.getValue()));
            }
            System.out.println("static压缩后:" + bitMap);
//            System.out.println(length);
//            // 解压缩
//            for (Map.Entry<String, BitSet> stringBitSetEntry : bitMap.entrySet()) {
//                decoding(stringBitSetEntry.getValue(),length.get(stringBitSetEntry.getKey()));
//            }
//            System.out.println(bitMap);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            JDBCUtils.close(rs, pstmt, conn);
        }
    }

    public static void main(String[] args) {
        System.out.println("hello");
//        selectMultiCode(new String[]{"F","L1","Jieyang"});
//        System.out.println(insert(new String[]{"Jon", "M", "Guangzhou", "L1"}));
//        System.out.println(bitMap);
//        System.out.println(update(new String[]{"name", "Cai", "gender", "F", "address", "Jieyang", "9"}));
//        System.out.println(update(new String[]{"name", "Tod", "address", "Guangzhou", "10"}));
//        System.out.println(update(new String[]{"name", "Pray", "address", "Shenzheng", "10"}));
        System.out.println(delete(36));
//        System.out.println(bitMap);
//        System.out.println(bitMap.get("Tod"));
//        byte[] bytes = bitMap.get("Tod").toByteArray();
//        for (byte b : bytes) {
//            System.out.println(b);
//        }
//        byte[] by = {0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1};
//        System.out.println(by.toString());
//        System.out.println(BitSet.valueOf(by));
//        for (int i = bytes.length - 1; i >= 0; i--) {
////            System.out.println(bytes[i]);
//            while (bytes[i]!=0){
//
//            }
//        }
//        byte[] bytes = "0101010101".getBytes();
//        byte[] by = new byte[]{4,64};
//        BitSet bitSet = BitSet.valueOf(by);
//        System.out.println(bitSet);
//        byte[] byteArray = bitSet.toByteArray();
//        for (byte b : byteArray) {
//            System.out.println(b);
//        }
//        for (byte b : bytes) {
//            System.out.println(b-'0');
//
//        }
//        encoding(bitMap.get("M"));
//        encoding(bitMap.get("F"));
//        int length = encoding(bitMap.get("L1"));
//        decoding(bitMap.get("L1"),length);
//        System.out.println(bitMap.get("L1"));
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
//        count++;
        // 执行insert操作
        // insert into 表名 values(值1,值2,...值n);
        StringBuffer sb = new StringBuffer("INSERT INTO " + table + " VALUES(NULL, ");
        for (int i = 1; i < insertStr.length; i++) {
            sb.append("?, ");
        }
        sb.append("?)");
        if (executeSqlUpdate(sb.toString(), insertStr)) {
            try {
                // 获取连接对象
                conn = JDBCUtils.getConnection();
                // 定义sql语句
                String sql = "SELECT MAX(id) FROM " + table;
                // 获取执行对象
                pstmt = conn.prepareStatement(sql);
                // 得到结果
                rs = pstmt.executeQuery();
                // 查询插入值的id号
                rs.next();
                int ID = rs.getInt(1);
                System.out.println(ID);

                // 判断插入的值是否在bitMap中
                for (String s : insertStr) {
                    BitSet bitSet = bitMap.get(s);
                    if (bitSet != null) {
                        // 是，则在其位置标1
                        // 解压缩
                        decoding(bitSet, length.get(s));
                        System.out.println(bitSet);
                        bitSet.set(ID);
                        length.put(s, encoding(bitSet));
//                        System.out.println(bitSet);
//                        System.out.println(length.get(s));
                    } else {
                        // 否，则在lists对应的列名中添加value，
                        bitSet = new BitSet();
                        bitSet.set(ID);
                        // 压缩并插入压缩后长度
                        length.put(s, encoding(bitSet));
//                        System.out.println(bitSet);
//                        System.out.println(length.get(s));
                        // 在bitMap中插入一个键值对
                        bitMap.put(s, bitSet);
                    }
                }
                return true;
            } catch (SQLException e) {
                e.printStackTrace();
                return false;
            } finally {
                JDBCUtils.close(rs, pstmt, conn);
            }
        } else {
            return false;
        }
    }


    /**
     * @param id 记录的id号
     * @Description: 根据id删除记录，并把位图索引删除
     * @Method: delete
     * @Implementation: 遍历每个bitMap，解压之后查看是否id位有值，如果有则删除，然后压缩
     * @Return: boolean 删除成功返回true,否则返回false
     * @Date: 2019/11/21 9:41
     * @Author: Tod
     */
    private static boolean delete(int id) {
//        DELETE FROM custom_info WHERE id=8;
        StringBuilder sb = new StringBuilder("DELETE FROM " + table + " WHERE id=?");
        if (executeSqlUpdate(sb.toString(), new String[]{String.valueOf(id)})) {
            for (Map.Entry<String, BitSet> stringBitSetEntry : bitMap.entrySet()) {
                String s = stringBitSetEntry.getKey();
                BitSet bitSet = stringBitSetEntry.getValue();
                // 解压缩
                decoding(bitSet, length.get(s));
                // 删除对应位
                bitSet.clear(id);
                length.put(s, encoding(bitSet));
            }
            return true;
        } else {
            return false;
        }
    }

    /**
     * @param updateStr 更新的字段及值及id号构成的字符串
     * @Description: 更新操作
     * @Method: update
     * @Implementation: 构造sql语句，根据id号，先修改之前的bitMap值，然后执行sql语句后，再通过id号，修改之后的bitMap值
     * @Return: boolean     成功返回true，否则返回false
     * @Date: 2019/11/21 9:44
     * @Author: Tod
     */
    private static boolean update(String[] updateStr) {
        // update 表名 set 列名1 = 值1, 列名2 = 值2,... [where 条件];
        // UPDATE custom_info SET NAME = "Jimmy" WHERE id = 7
        StringBuilder sb = new StringBuilder("UPDATE " + table + " SET ");
        String[] stringValue = new String[(updateStr.length >> 1) + 1];
//        String[] stringKey = new String[(updateStr.length>>1)];
        for (int i = 0; i < updateStr.length - 1; i = i + 2) {
            sb.append(updateStr[i] + "=?,");
            stringValue[i >> 1] = updateStr[i + 1];
//            stringKey[i>>1] = updateStr[i];
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(" WHERE id=?");
        stringValue[stringValue.length - 1] = updateStr[updateStr.length - 1];
        // 获取id
        int id = Integer.valueOf(updateStr[updateStr.length - 1]);
        // 修改之前的bitMap
        try {
            conn = JDBCUtils.getConnection();
            ResultSet rs = select(id);
            rs.next();
            for (int j = 2; j <= metaData.getColumnCount(); j++) {
                // 获取id的每一列的值
                String s = rs.getString(j);
                // 获取解压缩后的位图
                decoding(bitMap.get(s), length.get(s));
                // 清除id对应位的位图
                bitMap.get(s).clear(id);
                // 压缩位图并将压缩后的长度更新到length中
                length.put(s, encoding(bitMap.get(s)));
            }
            System.out.println("update压缩后：" + bitMap);
            if (executeSqlUpdate(sb.toString(), stringValue)) {
                // 判断插入的值是否在bitMap中
                conn = JDBCUtils.getConnection();
                // 查找修改后的值
                rs = select(id);
                rs.next();
                for (int j = 2; j <= metaData.getColumnCount(); j++) {
                    // 获取对应列的值
                    String s = rs.getString(j);
                    // 判断是否在bitMap中
                    BitSet bitSet = bitMap.get(s);
                    if (bitSet != null) {
                        // 解压缩
                        decoding(bitSet, length.get(s));
                        // 是，则在其位置标1
                        bitSet.set(id);
                        // 压缩后更新压缩后长度
                        length.put(s, encoding(bitSet));
                    } else {
                        // 否，则在lists对应的列名中添加value，
                        // 在bitMap中插入一个键值对
                        bitSet = new BitSet();
                        bitSet.set(id);
                        // 压缩后插入压缩后长度
                        length.put(s, encoding(bitSet));
                        // 在bitMap中插入新值
                        bitMap.put(s, bitSet);
                    }
                }
                JDBCUtils.close(rs, pstmt, conn);   //关闭连接
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

    /**
     * @param selectCondition 多个码值的字符串数组
     * @Description: 传入多个码值进行查询，输出相应结果
     * @Method: selectMultiCode
     * @Implementation: 构造bitSets集记录字符串对应解码后的bitMap中的BitSet值，先调用getSelectID得到id的List数组，再传入selectResult查询
     * @Return: void
     * @Date: 2019/11/21 11:53
     * @Author: Tod
     */
    private static void selectMultiCode(String[] selectCondition) {
        int l = selectCondition.length;
        BitSet[] bitSets = new BitSet[l];
        for (int i = 0; i < l; i++) {
            String s = selectCondition[i];
            decoding(bitMap.get(s), length.get(s));
            bitSets[i] = new BitSet();
            bitSets[i].or(bitMap.get(s));
            encoding(bitMap.get(s));
        }
        selectResult(getSelectID(bitSets));
    }

    /**
     * @param list 保存id号的list表
     * @Description: 根据list的id号来查询对应的结果
     * @Method: selectResult
     * @Implementation: 调用select函数查找结果集rs，调用print函数打印结果集
     * @Return: void
     * @Date: 2019/11/21 9:48
     * @Author: Tod
     */
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

    /**
     * @param rs ResultSet结果集
     * @Description: 通过结果集rs打印输出结果
     * @Method: print
     * @Implementation: 根据metaData中对应的列数目来查找对应的列值并打印输出
     * @Return: void
     * @Date: 2019/11/21 9:51
     * @Author: Tod
     */
    private static void print(ResultSet rs) throws SQLException {
        while (rs.next()) {
            for (int j = 1; j <= metaData.getColumnCount(); j++) {
                System.out.print(rs.getString(j) + '\t' + '\t');
            }
            System.out.println();
        }
    }

    /**
     * @param bitSet 待压缩的位图
     * @Description: 将bitSet进行位图压缩
     * @Method: encoding
     * @Implementation: 调用getList得到压缩序列，调用getBit得到序列中每个值的n = ceil(log m)值，设置前n-1个值为1，第n个值为0，后跟序列值的二进制位，返回压缩后的长度
     * @Return: int // 返回压缩后的长度
     * @Date: 2019/11/21 11:46
     * @Author: Tod
     */
    private static int encoding(BitSet bitSet) {
//        System.out.println(bitSet);
        // 得到压缩序列
        List<Integer> list = getList(bitSet);
//        System.out.println(list);
        // 当前下标
        int index = 0;
//        BitSet set = new BitSet();
        bitSet.clear();
        for (Integer integer : list) {
            // 得到每个值的n = ceil(log m)值
            int count = getBit(integer);
            int temp = count;
            // 0->0 1->1    2->2    3->2    4->3    5->3    ... 8->4
            // 设置前n-1个值为1
            while (count > 1) {
                bitSet.set(index++);
                count--;
            }
            // 设置第n个值为0
            index++;
            for (int i = temp - 1; i >= 0; i--) {
                // 移位操作
                if (((integer >> i) & 1) == 1) {
                    bitSet.set(index++);
                } else {
                    index++;
                }
            }
        }
//        System.out.println(bitSet);
//        System.out.println(index);
//        decoding(set,index);
        // 返回压缩后的长度，避免出现末尾多个0的情况被忽略
        return index;
    }

    /**
     * @param integer
     * @Description: 获取integer的二进制位数
     * @Method: getBit
     * @Implementation: 获取ceil(log2 ( integer + 1))的值，当integer为0时返回1
     * @Return: int 返回获取integer的二进制位数
     * @Date: 2019/11/21 12:09
     * @Author: Tod
     */
    private static int getBit(Integer integer) {
        if (integer == 0) {
            return 1;
        }
        return (int) Math.ceil(Math.log(integer + 1) / Math.log(2.0));
    }

    /**
     * @param bitSet 待压缩的BitSet值
     * @Description: 获取压缩序列，如100000001 -> (0,7)
     * @Method: getList
     * @Implementation: 两个指针一个gap, gap = i - j -1;
     * @Return: java.util.List<java.lang.Integer>
     * @Date: 2019/11/21 12:30
     * @Author: Tod
     */
    private static List<Integer> getList(BitSet bitSet) {
        List<Integer> list = new LinkedList<Integer>();
        int j = -1;
        int gap;
        for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
            gap = i - j - 1;
            j = i;
            list.add(gap);
        }
        return list;
    }

    /**
     * @param bitSet 压缩后的bitSet
     * @param length 压缩后的长度
     * @Description: 解压缩
     * @Method: decoding
     * @Implementation: 两个指针，一个移动指针来判断是否结束，一个恢复指针用来置位
     * @Return: void
     * @Date: 2019/11/21 12:16
     * @Author: Tod
     */
    private static void decoding(BitSet bitSet, int length) {
        BitSet set = new BitSet();
        // 移动指针     二进制位数   值
        int index = 0, count, num;
        // 恢复的指针
        int setIndex = 0;
        // 00110111 -> 100000001
        while (index < length) {
            count = 1;
            while (bitSet.get(index)) {
                count++;
                index++;
            }
            index++;
            num = 0;
            while (count > 0) {
                num <<= 1;
                num += bitSet.get(index) ? 1 : 0;
                count--;
                index++;
            }
            setIndex += num;
            set.set(setIndex++);
        }
//        System.out.println(set);
        bitSet.clear();
        bitSet.or(set);
//        System.out.println(bitSet);
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
