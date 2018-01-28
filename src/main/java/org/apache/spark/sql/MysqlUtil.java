package org.apache.spark.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;

import org.apache.spark.mllib.recommendation.Rating;

import scala.Tuple2;

public class MysqlUtil {
	

	public static void saveWC(Iterator<Tuple2<String, Integer>> r) {
		
		Connection con = null;
		PreparedStatement  pst = null;
		try{
			con = DriverManager.getConnection("jdbc:mysql://192.168.10.41:3306/test", "root", "huize123");
			String sql = "insert into t_word_wc(word_key,word_count)values(?,?)";
			pst = con.prepareStatement(sql);
			int i=1;
			while(r.hasNext()){
				System.out.println("处理正在进行=====");
				String word_key = r.next()._1;
				Integer count = r.next()._2;
				pst.setString(1, word_key);
				pst.setInt(2, count);
				pst.addBatch();
				i++;
				if(i %100==0){
					pst.executeBatch();
				}
				
			}
			pst.executeBatch();
			con.commit();
			pst.clearBatch();
			
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			if(con !=null){
				try {
					con.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if(pst !=null){
				try {
					pst.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		
		
	}

	
	public static void saveUserRecommend(Rating[] results) {
		Connection con = null;
		PreparedStatement  pst = null;
		try{
			con = DriverManager.getConnection("jdbc:mysql://192.168.10.41:3306/test", "root", "huize123");
			String sql = "insert into t_user_recommend(user_id,product_id,rating)values(?,?,?)";
			pst = con.prepareStatement(sql);
			int i=1;
			for(Rating r : results){
				System.out.println("处理正在进行=====");
				Integer user_id = r.user();
				Integer product_id = r.product();
				double rating = r.rating();
				pst.setInt(1, user_id);
				pst.setInt(2, product_id);
				pst.setDouble(3, rating);
				pst.addBatch();
				i++;
				if(i %100==0){
					pst.executeBatch();
				}
				
			}
			pst.executeBatch();
		//	con.commit();
			pst.clearBatch();
			
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			if(con !=null){
				try {
					con.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if(pst !=null){
				try {
					pst.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		
		
	}

}
