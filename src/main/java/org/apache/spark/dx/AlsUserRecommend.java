package org.apache.spark.dx;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.sql.MysqlUtil;

public class AlsUserRecommend {
	
	@SuppressWarnings("resource")
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setAppName("AlsUserRecommend");
		JavaSparkContext context = new JavaSparkContext(conf);
		
		//获取数据转换成RDD，1、hdfs，2、SparkSql、3、本地文件系统
      JavaRDD<String>order = context.textFile("/spark/file/user-products.csv");
      
      final Map<Integer,Set<Integer>>userBuyProducts = new HashMap<Integer,Set<Integer>>();
      //获取用户产品订单的第一行
      final String first =  order.first();
      //把RDD的每一行转换成了一个Rating对象
      JavaRDD<Rating>  orderRating = order.filter(new Function<String, Boolean>() {
		@Override
		public Boolean call(String arg0) throws Exception {
			return arg0.equals(first) ?false:true;
		}
	}).map(new Function<String, Rating>() {
		@Override
		 public Rating call(String arg0) throws Exception {
			String[] up = arg0.split(",");
			Integer userId = Integer.valueOf(up[0]);
			Integer productId=Integer.valueOf(up[1]);
			Rating r = new Rating(userId, productId, Double.valueOf(up[2]));
			//假如包含此用户
			if(userBuyProducts.containsKey(userId)){
				Set<Integer> productIds = userBuyProducts.get(userId);
				productIds.add(productId);
			}else{
				Set<Integer> productIds = new HashSet<Integer>();
				productIds.add(productId);
				userBuyProducts.put(userId, productIds);
			}
			
			return r;
		   }
	   });
      orderRating.cache();
      //RDD转换成ALS模型
      final MatrixFactorizationModel model  = ALS.train(orderRating.rdd(), 10, 10,0.01);
      
      final Map<Integer,Integer> userMap = new HashMap<Integer,Integer>();
      
      JavaRDD<String> userHotProducts = context.textFile("/spark/file/user-hot-products.csv");
      
      
      
      Rating[] results = model.recommendProducts(33239,10);
      System.out.println("推荐产品个数==="+results.length);
			 //判断此款产品是否已经购买
			 Set<Integer>products = userBuyProducts.get(33239);
			 System.out.println("========开始进入推荐产品判断================");
			// if(!products.contains(result.product())){
				 //把数据存储到mysql
				 System.out.println("========开始进入mysql================");
				 MysqlUtil.saveUserRecommend(results);
				 
			 //}
			 
      
//      userHotProducts.map(new Function<String, Boolean>() {
//		@Override
//		public Boolean call(String r) throws Exception {
//			System.out.println("========开始================"+r);
//			Integer uId = Integer.valueOf(r.split(",")[0]);
//			if(!userMap.containsKey(uId)){
//				 //给用户推荐十款比较感兴趣的产品
//				
//			}
//		 userMap.put(uId, 1);
//		
//			return null;
//		}
//	}).collect();
 
  
      
		
		
		
	}

}
