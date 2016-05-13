package com.hbase.test;


import java.io.IOException;  
import java.util.ArrayList;  
import java.util.List;  
  
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.hbase.HBaseConfiguration;  
import org.apache.hadoop.hbase.HColumnDescriptor;  
import org.apache.hadoop.hbase.HTableDescriptor;  
import org.apache.hadoop.hbase.KeyValue;  
import org.apache.hadoop.hbase.MasterNotRunningException;  
import org.apache.hadoop.hbase.ZooKeeperConnectionException;  
import org.apache.hadoop.hbase.client.Delete;  
import org.apache.hadoop.hbase.client.Get;  
import org.apache.hadoop.hbase.client.HBaseAdmin;  
import org.apache.hadoop.hbase.client.HTable;  
import org.apache.hadoop.hbase.client.HTablePool;  
import org.apache.hadoop.hbase.client.Put;  
import org.apache.hadoop.hbase.client.Result;  
import org.apache.hadoop.hbase.client.ResultScanner;  
import org.apache.hadoop.hbase.client.Scan;  
import org.apache.hadoop.hbase.filter.Filter;  
import org.apache.hadoop.hbase.filter.FilterList;  
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;  
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;  
import org.apache.hadoop.hbase.util.Bytes;  
  
public class HbaseDemo {  
  
    public static Configuration configuration;  
    static {  
        configuration = HBaseConfiguration.create();  
        configuration.set("hbase.zookeeper.property.clientPort", "2181");  
        configuration.set("hbase.zookeeper.quorum", "172.16.66.221");  
        configuration.set("hbase.master", "172.16.66.221:600000");  
    }  
  
    public static void main(String[] args) {  
         createTable("wujintao");  
        // insertData("wujintao");  
        // QueryAll("wujintao");  
        // QueryByCondition1("wujintao");  
        // QueryByCondition2("wujintao");  
        //QueryByCondition3("wujintao");  
        //deleteRow("wujintao","abcdef");  
       // deleteByCondition("wujintao","abcdef");  
    }  
  
    /** 
     * ������ 
     * @param tableName 
     */  
    public static void createTable(String tableName) {  
        System.out.println("start create table ......");  
        try {  
            HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration);  
            if (hBaseAdmin.tableExists(tableName)) {// �������Ҫ�����ı���ô��ɾ�����ٴ���  
                hBaseAdmin.disableTable(tableName);  
                hBaseAdmin.deleteTable(tableName);  
                System.out.println(tableName + " is exist,detele....");  
            }  
            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);  
            tableDescriptor.addFamily(new HColumnDescriptor("column1"));  
            tableDescriptor.addFamily(new HColumnDescriptor("column2"));  
            tableDescriptor.addFamily(new HColumnDescriptor("column3"));  
            hBaseAdmin.createTable(tableDescriptor);  
        } catch (MasterNotRunningException e) {  
            e.printStackTrace();  
        } catch (ZooKeeperConnectionException e) {  
            e.printStackTrace();  
        } catch (IOException e) {  
            e.printStackTrace();  
        }  
        System.out.println("end create table ......");  
    }  
  
    /** 
     * �������� 
     * @param tableName 
     */  
    public static void insertData(String tableName) {  
        System.out.println("start insert data ......");  
        HTablePool pool = new HTablePool(configuration, 1000);  
        HTable table = (HTable) pool.getTable(tableName);  
        Put put = new Put("112233bbbcccc".getBytes());// һ��PUT����һ�����ݣ���NEWһ��PUT��ʾ�ڶ�������,ÿ��һ��Ψһ��ROWKEY���˴�rowkeyΪput���췽���д����ֵ  
        put.add("column1".getBytes(), null, "aaa".getBytes());// �������ݵĵ�һ��  
        put.add("column2".getBytes(), null, "bbb".getBytes());// �������ݵĵ�����  
        put.add("column3".getBytes(), null, "ccc".getBytes());// �������ݵĵ�����  
        try {  
            table.put(put);  
        } catch (IOException e) {  
            e.printStackTrace();  
        }  
        System.out.println("end insert data ......");  
    }  
  
    /** 
     * ɾ��һ�ű� 
     * @param tableName 
     */  
    public static void dropTable(String tableName) {  
        try {  
            HBaseAdmin admin = new HBaseAdmin(configuration);  
            admin.disableTable(tableName);  
            admin.deleteTable(tableName);  
        } catch (MasterNotRunningException e) {  
            e.printStackTrace();  
        } catch (ZooKeeperConnectionException e) {  
            e.printStackTrace();  
        } catch (IOException e) {  
            e.printStackTrace();  
        }  
  
    }  
    /** 
     * ���� rowkeyɾ��һ����¼ 
     * @param tablename 
     * @param rowkey 
     */  
     public static void deleteRow(String tablename, String rowkey)  {  
        try {  
            HTable table = new HTable(configuration, tablename);  
            List list = new ArrayList();  
            Delete d1 = new Delete(rowkey.getBytes());  
            list.add(d1);  
              
            table.delete(list);  
            System.out.println("ɾ���гɹ�!");  
              
        } catch (IOException e) {  
            e.printStackTrace();  
        }  
          
  
    }  
  
     /** 
      * �������ɾ�� 
      * @param tablename 
      * @param rowkey 
      */  
     public static void deleteByCondition(String tablename, String rowkey)  {  
            //Ŀǰ��û�з�����Ч��API�ܹ�ʵ�� ���ݷ�rowkey������ɾ�� ��������ܣ�������ձ�ȫ�����ݵ�API����  
  
    }  
  
  
    /** 
     * ��ѯ�������� 
     * @param tableName 
     */  
    public static void QueryAll(String tableName) {  
        HTablePool pool = new HTablePool(configuration, 1000);  
        HTable table = (HTable) pool.getTable(tableName);  
        try {  
            ResultScanner rs = table.getScanner(new Scan());  
            for (Result r : rs) {  
                System.out.println("��õ�rowkey:" + new String(r.getRow()));  
                for (KeyValue keyValue : r.raw()) {  
                    System.out.println("�У�" + new String(keyValue.getFamily())  
                            + "====ֵ:" + new String(keyValue.getValue()));  
                }  
            }  
        } catch (IOException e) {  
            e.printStackTrace();  
        }  
    }  
  
    /** 
     * ��������ѯ,����rowkey��ѯΨһһ����¼ 
     * @param tableName 
     */  
    public static void QueryByCondition1(String tableName) {  
  
        HTablePool pool = new HTablePool(configuration, 1000);  
        HTable table = (HTable) pool.getTable(tableName);  
        try {  
            Get scan = new Get("abcdef".getBytes());// ����rowkey��ѯ  
            Result r = table.get(scan);  
            System.out.println("��õ�rowkey:" + new String(r.getRow()));  
            for (KeyValue keyValue : r.raw()) {  
                System.out.println("�У�" + new String(keyValue.getFamily())  
                        + "====ֵ:" + new String(keyValue.getValue()));  
            }  
        } catch (IOException e) {  
            e.printStackTrace();  
        }  
    }  
  
    /** 
     * ����������ѯ����ѯ������¼ 
     * @param tableName 
     */  
    public static void QueryByCondition2(String tableName) {  
  
        try {  
            HTablePool pool = new HTablePool(configuration, 1000);  
            HTable table = (HTable) pool.getTable(tableName);  
            Filter filter = new SingleColumnValueFilter(Bytes  
                    .toBytes("column1"), null, CompareOp.EQUAL, Bytes  
                    .toBytes("aaa")); // ����column1��ֵΪaaaʱ���в�ѯ  
            Scan s = new Scan();  
            s.setFilter(filter);  
            ResultScanner rs = table.getScanner(s);  
            for (Result r : rs) {  
                System.out.println("��õ�rowkey:" + new String(r.getRow()));  
                for (KeyValue keyValue : r.raw()) {  
                    System.out.println("�У�" + new String(keyValue.getFamily())  
                            + "====ֵ:" + new String(keyValue.getValue()));  
                }  
            }  
        } catch (Exception e) {  
            e.printStackTrace();  
        }  
  
    }  
  
    /** 
     * ���������ѯ 
     * @param tableName 
     */  
    public static void QueryByCondition3(String tableName) {
  
        try {  
            HTablePool pool = new HTablePool(configuration, 1000);  
            HTable table = (HTable) pool.getTable(tableName);  
  
            List<Filter> filters = new ArrayList<Filter>();  
  
            Filter filter1 = new SingleColumnValueFilter(Bytes  
                    .toBytes("column1"), null, CompareOp.EQUAL, Bytes  
                    .toBytes("aaa"));  
            filters.add(filter1);  
  
            Filter filter2 = new SingleColumnValueFilter(Bytes  
                    .toBytes("column2"), null, CompareOp.EQUAL, Bytes  
                    .toBytes("bbb"));  
            filters.add(filter2);  
  
            Filter filter3 = new SingleColumnValueFilter(Bytes  
                    .toBytes("column3"), null, CompareOp.EQUAL, Bytes  
                    .toBytes("ccc"));  
            filters.add(filter3);  
  
            FilterList filterList1 = new FilterList(filters);  
  
            Scan scan = new Scan();  
            scan.setFilter(filterList1);  
            ResultScanner rs = table.getScanner(scan);  
            for (Result r : rs) {  
                System.out.println("��õ�rowkey:" + new String(r.getRow()));  
                for (KeyValue keyValue : r.raw()) {  
                    System.out.println("�У�" + new String(keyValue.getFamily())  
                            + "====ֵ:" + new String(keyValue.getValue()));  
                }  
            }  
            rs.close();  
  
        } catch (Exception e) {  
            e.printStackTrace();  
        }  
  
    }  
  
}  
