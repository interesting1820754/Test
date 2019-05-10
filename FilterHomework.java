package com.Filter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class FilterHomework {

    Configuration configuration  = null;
    Connection connection = null;
    Table table = null;
    String tableName = null;

    //实例化
    public void init() throws Exception{
        configuration =HBaseConfiguration.create();
        connection = ConnectionFactory.createConnection(configuration);
        tableName = "jobs1"; //表名
        table =connection.getTable(TableName.valueOf(tableName));
    }

    //第一题
    public void one() throws Exception{
        Filter filter1 = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("company_industry"),
                CompareFilter.CompareOp.EQUAL,new SubstringComparator("互联网"));
        Filter filter2 = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("job_name")));
        FilterList list = new FilterList();
        list.addFilter(filter1);
        list.addFilter(filter2);
        Scan scan = new Scan();
        scan.setFilter(list);
        ResultScanner results = table.getScanner(scan);
        for(Result result :results){
            for(Cell cell:result.rawCells()) {
                System.out.println(Bytes.toString(cell.getValueArray()));
            }
        }
    }

    //第二题
    public void two() throws Exception{
        Filter filter1 = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("job_edu_require"),
                CompareFilter.CompareOp.EQUAL,new SubstringComparator("硕士"));
        Filter filter2 = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("job_info")));

        FilterList list = new FilterList();
        list.addFilter(filter1);
        list.addFilter(filter2);
        Scan scan = new Scan();
        scan.setFilter(list);


        ResultScanner results = table.getScanner(scan);
        for(Result result :results){
            for(Cell cell:result.rawCells()){
                System.out.println(Bytes.toString(cell.getValueArray()));

            }

        }
    }

    //第三题
    public void three() throws Exception{
        List<Filter> list = new ArrayList<Filter>();
        Filter filter1 = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("job_tag"),
                CompareFilter.CompareOp.EQUAL,new SubstringComparator("机器学习"));
        Filter filter2 = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("job_info")));

        Filter filter3 = new PageFilter(2);

        list.add(filter1);
        list.add(filter2);
        list.add(filter3);

        FilterList fl = new FilterList(list);
        Scan scan = new Scan();
        scan.setFilter(fl);
        ResultScanner results = table.getScanner(scan);

        for(Result result :results){
            for(Cell cell:result.rawCells()){
                System.out.println(Bytes.toString(cell.getValueArray()));

            }
            result.getRow();
        }
    }

    //第四题
    public void four() throws Exception{
        Filter filter1 = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("company_location"),
                CompareFilter.CompareOp.EQUAL,new SubstringComparator("上海"));

        Filter filter2 = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("company_location"),
                CompareFilter.CompareOp.EQUAL,new SubstringComparator("北京"));

        Filter filter３ = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("job_salary"),
                CompareFilter.CompareOp.EQUAL,new SubstringComparator("10k-20k"));

        Filter filter4 = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("job_info")));

        FilterList list1 = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        FilterList list2 = new FilterList(FilterList.Operator.MUST_PASS_ALL);

        list1.addFilter(filter1);
        list1.addFilter(filter2);
        list2.addFilter(filter３);
        list2.addFilter(list1);
        list2.addFilter(filter4);

        Scan scan = new Scan();
        //scan.addColumn(Bytes.toBytes("info"),Bytes.toBytes("job_info"));
        scan.setFilter(list2);
        ResultScanner results = table.getScanner(scan);
        int i =0;
        for(Result result :results){
            for(Cell cell:result.rawCells()){
                System.out.println(Bytes.toString(cell.getValueArray()));

            }
            i = i+1;
        }
        System.out.println(i);
    }

    //第五题
    public void five() throws Exception{

        Filter filter1 = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("company_location"),
                CompareFilter.CompareOp.EQUAL,new SubstringComparator("北京"));
        Filter filter2 = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("company_people"),
                CompareFilter.CompareOp.GREATER,new BinaryComparator(Bytes.toBytes("100")));
        Filter filter3 = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("job_info")));

        FilterList list1 = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        list1.addFilter(filter1);
        list1.addFilter(filter2);
        list1.addFilter(filter3);
        Scan scan = new Scan();
        scan.setFilter(list1);
        ResultScanner results = table.getScanner(scan);
        int i =0;
        for(Result result :results){
            for(Cell cell:result.rawCells()){
                System.out.println(Bytes.toString(cell.getValueArray()));

            }
            i = i+1;
        }
        System.out.println(i);
    }

    //关闭连接
    public void close() throws IOException {
        table.close();
        connection.close();
    }


    public static void main(String[] args) throws Exception{
        System.setProperty("hadoop.home.dir", "/usr/local/hadoop");
        FilterHomework filterHomework = new FilterHomework();
        filterHomework.init();
        filterHomework.one();
        //filterHomework.two();
        //filterHomework.three();
        //filterHomework.four();
        //filterHomework.five();
        filterHomework.close();
    }
}
