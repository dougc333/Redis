package com.absolute;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class TestSqlServer {

	public static void main(String []args){
		try{
			Class.forName("net.sourceforge.jtds.jdbc.Driver").newInstance();
			Connection conn=DriverManager.getConnection("jdbc:jtds:sqlserver://DV2CORP2/CCDATA;domain=Absolute;","dchang","Passw0rd2");
			if(conn==null){
				System.out.println("null connection");
			}
			String c = "select top 10 * from vwESNSummary";
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(c);
			while(rs.next()){
				System.out.println("rs:"+rs.getString(1));
			}
		}catch(Exception e){
			e.printStackTrace();
		}	}
}
