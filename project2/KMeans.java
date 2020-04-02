import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Scanner;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.bloom.Key;


class Point implements WritableComparable<Point> {
    public Double x;
    public Double y;
    public Point()
    {
    	this.x=0.0;
    	this.y=0.0;
    }
    public Point(double a,double b)
    {
    	this.x=a;
    	this.y=b;
    }
	@Override
	public void readFields(DataInput i) throws IOException {
		// TODO Auto-generated method stub
		x = i.readDouble();
		y = i.readDouble();
	}
	@Override
	public void write(DataOutput o) throws IOException {
		// TODO Auto-generated method stub
		o.writeDouble(x);
		o.writeDouble(y);
	}
	@Override
	public int compareTo(Point p) {
		// TODO Auto-generated method stub
		if(Double.compare(this.x, p.x)==0)							
			return (int) (this.y- p.y);				
		else 
			return (int) (this.x-p.x);
	}
	public String toString()
	{
		return Double.toString(x)+","+Double.toString(y);
	}		
}

class Avg implements Writable{
	public double sumx;
	public double sumy;
	public double count;
	public Avg()
	{
		this.sumx=0;
		this.sumy=0;
		this.count=1;
	}
	public Avg(double a,double b,double c)
	{
		this.sumx=a;
		this.sumy=b;
		this.count=c;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		sumx = in.readDouble();
		sumy = in.readDouble();
		count = in.readDouble();
	}
	@Override
	public void write(DataOutput o) throws IOException {
		// TODO Auto-generated method stub
		o.writeDouble(sumx);
		o.writeDouble(sumy);
		o.writeDouble(count);
	}	
	public String toString()
	{
		return Double.toString(sumx)+" "+Double.toString(sumy)+" "+Double.toString(count);
	}
}

public class KMeans {
	static Vector<Point> centroids = new Vector<Point>(100);
	static Hashtable<Point, Avg> table;
	
    public static class AvgMapper extends Mapper<Object,Text,Point,Avg> {
    	@Override
    	public void setup(Context context) throws IOException, InterruptedException
    	{    	    		    	
    		table = new Hashtable<Point, Avg>();
    		Configuration conf = new Configuration();
    		URI[] paths = context.getCacheFiles();
    		FileSystem fs = FileSystem.get(conf); 
    		Path path = new Path(paths[0]);
    		InputStreamReader isr = new InputStreamReader(fs.open(path));
    		BufferedReader reader = new BufferedReader(isr);
    		String str="";
    		table.clear();
    	 	while((str=reader.readLine())  != null)
    		{    	 		    	 	
    			Point p = new Point(Double.parseDouble(str.split(",")[0]),Double.parseDouble(str.split(",")[1]));    			
    			centroids.add(p);    		
    		}    	    	 	    	 	    	 
    	}
    	public void cleanup(Context context) throws IOException, InterruptedException
    	{    		
    		for(Point p:table.keySet())
    		{
    			context.write(p, table.get(p));    			
    		}
    	}
    	public void map(Object key,Text value,Context context) throws IOException, InterruptedException
    	{    		    		
    		centroids.firstElement();    		
    		Scanner sc = new Scanner(value.toString()).useDelimiter(",");
    		double min_dis = 10000;
    		double temp = 0;
    		Point p2 = new Point();    		
    		Point p1 = new Point(sc.nextDouble(),sc.nextDouble());    	 
    		
    		for(Point p:centroids)
    		{    	    			    		    			
    			temp = Math.sqrt(Math.pow(Math.abs(p.x-p1.x), 2)+Math.pow(Math.abs(p.y-p1.y), 2));
    			if(temp<min_dis)
    			{        								
    				min_dis = temp;    				
    				p2 = p;    			    				
    			}    			    			
    		}
    		Avg a = new Avg();
    		if(table.get(p2)!=null)
    		{    			    			
    			a = table.get(p2);
    			a.sumx += p1.x;
    			a.sumy += p1.y;
    			a.count += 1;
    			table.put(p2, new Avg(a.sumx,a.sumy,a.count));
    			
    		}
    		else
    			table.put(p2,new Avg(p1.x,p1.y,1));    	 		 		    	
    	}
    }
    
    public static class AvgReducer extends Reducer<Point,Avg,Text,NullWritable> {
    	public void reduce(Point key,Iterable<Avg> value,Context context) throws IOException, InterruptedException 
    	{   
    		double c=0;
    		Point s = new Point();
    		for(Avg a:value)
    		{
    			c += a.count;
    			s.x+=a.sumx;
    			s.y+=a.sumy;
    		}
    		s.x=s.x/c;
    		s.y=s.y/c;
    		    		
        	context.write(new Text(s.toString()),null);        	        	        
    	}
    	
    }


	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		// TODO Auto-generated method stub	
		Configuration conf  = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(KMeans.class);
		
        job.setJobName("KMeans");
        
        job.addCacheFile(new Path(args[1]).toUri());                        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullOutputFormat.class);
        job.setMapOutputKeyClass(Point.class);
        job.setMapOutputValueClass(Avg.class);
        
        job.setMapperClass(AvgMapper.class);        
        job.setReducerClass(AvgReducer.class);   
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);        
        
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[2]));
        
        job.waitForCompletion(true);
	}

}
