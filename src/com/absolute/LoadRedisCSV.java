package com.absolute;



import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import redis.clients.jedis.Jedis;

//2 conventions, 1) store null as NULL
// 2) leave null off, dont store the field. 
public class LoadRedisCSV {
	private static List<String> headerList;
	private static Jedis jedis = new Jedis("localhost");
	static private String firstLine;
	
	private static void init(){
		headerList = new ArrayList<String>();
	}
	
	
	private static void parseHeader(String header){
		System.out.println("header:"+header);
		firstLine = header;
		StringTokenizer st = new StringTokenizer(header, "^");
		while(st.hasMoreTokens()){
			headerList.add(st.nextToken());
		}
		System.out.println("headerList size:"+headerList.size());
	}
	
	//assume first line is a header
	private static void loadFileasHM(String tableName, String file){
		try{
			init();
			BufferedReader br = new BufferedReader(new FileReader(file));
			String fileLine = null;
			String header = br.readLine();
			parseHeader(header);
			
			int numMatch=0;
			int numNoMatch=0;
			int numRows=0;
			
			while((fileLine=br.readLine())!=null){
				StringTokenizer st = new StringTokenizer(fileLine,"^");
				List<String> dataList = new ArrayList<String>();			
//				System.out.println("firstLine:"+firstLine);
//				System.out.println("fileline:"+fileLine);
				
				while(st.hasMoreTokens()){
					String next =  st.nextToken();
//					System.out.println("next"+next);
					dataList.add(next);
				}
				System.out.println("dataList size:"+dataList.size());
				
				
				if(headerList.size()!=dataList.size()){
			//		System.out.println("header size not match data size:"+headerList.size()+" , "+dataList.size());
			//		System.out.println("-----------------");
			//		System.out.println("firstLine:"+firstLine);
			//		System.out.println("fileline:"+fileLine);					
			//		System.out.println("-----------------");
			//		for(String s:dataList){
			//			System.out.println("dataList:"+s);
			//		}
			//		System.exit(0);
					//match:936
					//no match:33
					numNoMatch++;
				}else{
					//match, convert to hm in redis and load. 
					//foramt tablename_Row#, key:value = columnname:value in column
					//read the columnane:value into a HashMap then enter in
					System.out.println("match!!!!!");
					numMatch++;
					Map<String,String> map = new TreeMap<String,String>();
					int numCols=0;
					for(String h:headerList){
						map.put(h, dataList.get(numCols));
						numCols++;
					}
					System.out.println("inserting to:"+tableName+numRows);
					jedis.hmset(tableName+numRows, map);
					numRows++;
				}
			}
			System.out.println("match:"+numMatch);
			System.out.println("no match:"+numNoMatch);

		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public static void main(String []args){
		jedis.select(0);
		jedis.flushDB();
		loadFileasHM("vwDDDeviceByGroup","/home/dc/storm/vwDDDeviceByGroup.csv");
		loadFileasHM("vwDDDevice","/home/dc/storm/vwDDDevice.csv");
		//run in redis-cli
		//hmget "vwDDDevice1" ESN
		//hmget "vwDDDeviceByGroup1" ESN

	}
}