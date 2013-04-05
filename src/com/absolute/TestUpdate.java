package com.absolute;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class TestUpdate {

	public static void main(String []args){
		try{
			Class.forName("net.sourceforge.jtds.jdbc.Driver").newInstance();
			Connection conn=DriverManager.getConnection("jdbc:jtds:sqlserver://DV2CORP2/CCDATA;domain=Absolute;","dchang","Passw0rd2");
			if(conn==null){
				System.out.println("null connection");
			}
			String c = "use ccdata;";
			Statement stmt = conn.createStatement();
			int resultUseDB = stmt.executeUpdate(c);
			System.out.println("resultDB:"+resultUseDB);
			
			String sql = "insert into [DeviceLock].[BulkRequestQueue_DC] values('100')";
			int result = stmt.executeUpdate(sql);
			System.out.println(result);

			
			
		}catch(Exception e){
			e.printStackTrace();
		}
	}
}
