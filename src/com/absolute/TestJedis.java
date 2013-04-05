package com.absolute;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import redis.clients.jedis.Jedis;


public class TestJedis {
	private static Jedis jedis;
	
	private void foo(){
		while(true){
			if(jedis.exists("deviceready1")){
				System.out.println("exists");
				System.exit(0);
			}else{
				System.out.println("not exists");
			}
		}
	}
	
	public static void main(String []args){
		try{
			jedis = new Jedis("localhost");
			jedis.set("key", "value");
			System.out.println("get:"+jedis.get("key"));
			//get the device ESNs.. make sure you can retrieve this. 
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
				
				
				//print out groupIDMap stats
				//we need a list of the devices per groupID
				
				//test rpush in redis:
				
				//test rpop in redis
			}
			Set<String> groupIDKeys = groupIDMap.keySet();
			for(String groupIDs:groupIDKeys){
				System.out.println("groupID:"+groupIDs+" numvwDDDGroups:"+groupIDMap.get(groupIDs));
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
			
		}catch(Exception e){
			e.printStackTrace();
		}
	}
}
