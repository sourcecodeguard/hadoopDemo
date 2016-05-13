package com.hdfs.test;
import java.io.IOException;  
import java.net.URI;  
import java.net.URISyntaxException;  
  
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.FSDataInputStream;  
import org.apache.hadoop.fs.FSDataOutputStream;  
import org.apache.hadoop.fs.FileStatus;  
import org.apache.hadoop.fs.FileSystem;  
import org.apache.hadoop.fs.FileUtil;  
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.IOUtils;  
public class HdfsDemo {
	   //��ָ��λ���½�һ���ļ�����д���ַ�  
    public static void WriteToHDFS(String file, String words) throws IOException, URISyntaxException  
    {  
        Configuration conf = new Configuration();  
        FileSystem fs = FileSystem.get(URI.create(file), conf);  
        Path path = new Path(file);  
        FSDataOutputStream out = fs.create(path);   //�����ļ�  
  
        //���������������ļ�д�룬����һ���ʹ�ú���  
        out.writeBytes(words);    
        out.write(words.getBytes("UTF-8"));  
          
        out.close();  
        //�����Ҫ����������д�룬���Ǵ�һ���ļ�д����һ���ļ�����ʱ�����������������ݵ��ļ���  
        //����ʹ������IOUtils.copyBytes������  
        //FSDataInputStream in = fs.open(new Path(args[0]));  
        //IOUtils.copyBytes(in, out, 4096, true)        //4096Ϊһ�θ��ƿ��С��true��ʾ������ɺ�ر���  
    }  
      
    public static void ReadFromHDFS(String file) throws IOException  
    {  
        Configuration conf = new Configuration();  
        FileSystem fs = FileSystem.get(URI.create(file), conf);  
        Path path = new Path(file);  
        FSDataInputStream in = fs.open(path);  
          
        IOUtils.copyBytes(in, System.out, 4096, true);  
        //ʹ��FSDataInoutStream��read�����Ὣ�ļ����ݶ�ȡ���ֽ����в�����  
        /** 
         * FileStatus stat = fs.getFileStatus(path); 
      // create the buffer 
       byte[] buffer = new byte[Integer.parseInt(String.valueOf(stat.getLen()))]; 
       is.readFully(0, buffer); 
       is.close(); 
             fs.close(); 
       return buffer; 
         */  
    }  
      
    public static void DeleteHDFSFile(String file) throws IOException  
    {  
        Configuration conf = new Configuration();  
        FileSystem fs = FileSystem.get(URI.create(file), conf);  
        Path path = new Path(file);  
        //�鿴fs��delete API���Կ�������������deleteonExitʵ���˳�JVMʱɾ��������ķ�������ָ��ΪĿ¼�ǵݹ�ɾ��  
        fs.delete(path,true);  
        fs.close();  
    }  
      
    public static void UploadLocalFileHDFS(String src, String dst) throws IOException  
    {  
        Configuration conf = new Configuration();  
        FileSystem fs = FileSystem.get(URI.create(dst), conf);  
        Path pathDst = new Path(dst);  
        Path pathSrc = new Path(src);  
          
        fs.copyFromLocalFile(pathSrc, pathDst);  
        fs.close();  
    }  
      
    public static void ListDirAll(String DirFile) throws IOException  
    {  
        Configuration conf = new Configuration();  
        FileSystem fs = FileSystem.get(URI.create(DirFile), conf);  
        Path path = new Path(DirFile);  
          
        FileStatus[] status = fs.listStatus(path);  
        //����1    
        for(FileStatus f: status)  
        {  
            System.out.println(f.getPath().toString());    
        }  
        //����2    
        Path[] listedPaths = FileUtil.stat2Paths(status);    
        for (Path p : listedPaths){   
          System.out.println(p.toString());  
        }  
    }  
      
    public static void main(String [] args) throws IOException, URISyntaxException  
    {  
        //������������ʾĿ¼�������ļ�  
        ListDirAll("hdfs://172.16.66.221:9100/");  
          
        String fileWrite = "hdfs://172.16.66.221:9100/user/kqiao/test/FileWrite";  
        String words = "This words is to write into file!\n";  
        WriteToHDFS(fileWrite, words);  
        //�������Ƕ�ȡfileWrite�����ݲ���ʾ���ն�  
        ReadFromHDFS(fileWrite);  
        //����ɾ�������fileWrite�ļ�  
        DeleteHDFSFile(fileWrite);  
        //���豾����һ��uploadFile�������ϴ����ļ���HDFS  
//      String LocalFile = "file:///home/kqiao/hadoop/MyHadoopCodes/uploadFile";  
//      UploadLocalFileHDFS(LocalFile, fileWrite    );  
    }  
}
