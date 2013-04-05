package com.absolute;




import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;

import redis.clients.jedis.Jedis;

//2 conventions, 1) store null as NULL
// 2) leave null off, dont store the field. 
public class LoadRedisCSV {
	private static List<String> headerList;
	private static Jedis jedis;
	static private String firstLine;

	public LoadRedisCSV(){
		jedis = new Jedis("localhost");
	}
	
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
				
				while(st.hasMoreTokens()){
					String next =  st.nextToken();
					dataList.add(next);
				}
				System.out.println("dataList size:"+dataList.size());
				
				
				if(headerList.size()!=dataList.size()){
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
	
	private static void buildDataStructs(){
		Set<String> set = jedis.keys("vwDDDeviceByGroup*");
		ArrayList<String> temp = new ArrayList<String>();
		Map<String,List<String>> groupIDMap = new TreeMap<String,List<String>>();
		for(String st : set){
			System.out.println("st:"+st);
			Map<String,String> map = jedis.hgetAll(st);
			//these contain the groupIDs and ESNs
			System.out.println("map size:"+map.size());
			String groupID = map.get("GroupId");
			String ESN = map.get("ESN");
			System.out.println(st+" groupID:"+groupID+" ESN:"+ESN);
			if(groupIDMap.get(groupID)==null){
				ArrayList<String> al = new ArrayList<String>();
				al.add(ESN);
				groupIDMap.put(groupID,al);
			}else{
				List<String> al = groupIDMap.get(groupID);
				al.add(ESN);
			}
						
		}
		Set<String> groupIDKeys = groupIDMap.keySet();
		for(String groupIDs:groupIDKeys){
			System.out.println("groupID:"+groupIDs+" numvwDDDGroups:"+groupIDMap.get(groupIDs));
			//store in redis
			List<String> groupIDList = groupIDMap.get(groupIDs);
			for(String str:groupIDList){
				if(groupIDs.equals("555")){
					System.out.println("555 ENTERING IN push"+groupIDs+"  str:"+str);
				}
				jedis.rpush(groupIDs,str);
			}
		}
		//store the hm in redis. 
		
		
		System.out.println("num keys:"+set.size());
		System.out.println("num distinct groupIds:"+groupIDMap.size());
		
		System.out.println("pushing elements to list testpush");
		//			
		jedis.del("testpush"); 
		jedis.rpush("testpush", "1","2","3");
		System.out.println("jedis testpush length:"+jedis.llen("testpush"));
		System.out.println("jedis range:"+jedis.lpop("testpush"));
		
		System.out.println("groupID 555:"+groupIDMap.get("555"));
		//do we need to push the lists into  redis? 
		System.out.println("555 len:"+jedis.llen("555"));
		
		for(int i=0;i<jedis.llen("555");i++){
			System.out.println("i:"+i);
			System.out.println(jedis.lpop("555"));
			System.out.println(jedis.rpop("555"));
		}

	}
	
	public static void main(String []args){
		new LoadRedisCSV();
		jedis.select(0);
		jedis.flushDB();
		loadFileasHM("vwDDDeviceByGroup","/home/dc/storm/vwDDDeviceByGroup.csv");
		loadFileasHM("vwDDDevice","/home/dc/storm/vwDDDevice.csv");
		//run in redis-cli
		//hmget "vwDDDevice1" ESN
		//hmget "vwDDDeviceByGroup1" ESN
		System.out.println("db size:"+jedis.dbSize());
		buildDataStructs();
	}
}