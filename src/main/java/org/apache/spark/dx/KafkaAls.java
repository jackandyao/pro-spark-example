package org.apache.spark.dx;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.MysqlUtil;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

public class KafkaAls {
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("KafkaAls");
		StreamingContext ssc = new StreamingContext(conf, new Duration(3000));
		JavaStreamingContext jsc= new JavaStreamingContext(ssc );
		String zkQuorum = "192.168.10.141:2181/kafka";
		String groupId="spark";
		Map<String, Integer> topicMap = new HashMap<String, Integer>();
		topicMap.put("test", 2);
		
	    JavaSparkContext context =JavaSparkContext.fromSparkContext(jsc.sparkContext().sc()) ;
		JavaRDD<String>order = context .textFile("/spark/file/user-products.csv");
		final String first = order.first();
		JavaRDD<String> originalRdd = order.filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String v1) throws Exception {
				return v1.equals(first)?false: true;
			}
		});
		
		final JavaRDD<Rating> orginalRatings = originalRdd.map(new Function<String, Rating>() {
			@Override
			public Rating call(String v1) throws Exception {
				String[] userProducts = v1.split(",");
				Integer userId = Integer.valueOf(userProducts[0]);
				Integer productId = Integer.valueOf(userProducts[1]);
				double rating = Double.valueOf(userProducts[2]);
				Rating r = new Rating(userId,productId,rating);
				return r;
			}
		});
		orginalRatings.cache();
		
		JavaPairReceiverInputDStream<String, String>  kafkaStream = KafkaUtils.createStream(jsc, zkQuorum, groupId, topicMap );
		//获取到kafka的Json值
		 JavaDStream<String>  kafkaJsonStream = kafkaStream.map(new Function<Tuple2<String,String>, String>() {
			@Override
			public String call(Tuple2<String, String> v1) throws Exception {
				return v1._2;
			}
		});
		 
		 
		 final SQLContext sqlContext = new SQLContext(jsc.sparkContext());
		 
		 kafkaJsonStream.foreachRDD(new VoidFunction<JavaRDD<String>> () {
			@Override
			public void call(JavaRDD<String> jsonRDD) throws Exception {

				DataFrame dataFrame = sqlContext.read().json(jsonRDD);
				if(dataFrame.columns()!=null && dataFrame.columns().length>0){
				DataFrame jsondf = sqlContext.read().json(jsonRDD).select("userId","productId","rating");
					jsondf.show();
				 JavaRDD<Rating>  ratingRdd = jsondf.toJavaRDD().map(new Function<Row, Rating>() {
					@Override
					public Rating call(Row v1) throws Exception {
						Rating r = new Rating((int)v1.getLong(0),(int)v1.getLong(1),Double.valueOf(v1.get(2).toString()));
						return r;
					}
				});
				 ratingRdd.cache();
				 ratingRdd.collect();
			
				 JavaRDD<Rating> allRating = orginalRatings.union(ratingRdd);
				 
				 recommend(allRating,ratingRdd);
				 ratingRdd.unpersist();
				}
			}
				 
//				 Rating first = allRating.first();
//				System.out.println("======"+first.user()+first.product()+first.rating());
		});
		
		 jsc.start();
		 jsc.awaitTermination();
		
	}

	
	//推荐
	protected static void recommend(JavaRDD<Rating> allRating,
			JavaRDD<Rating> ratingRdd) {
		final MatrixFactorizationModel model = ALS.train(allRating.rdd(), 10, 10);
		
//		ratingRdd.foreach(new VoidFunction<Rating>() {
//			@Override
//			public void call(Rating t) throws Exception {
//					Rating[] results = model.recommendProducts(t.user(), 6);
//		 MysqlUtil.saveUserRecommend(results);
//			}
//		});
		
		List<Rating> rs = ratingRdd.collect();
		for(Rating r : rs){
			Rating[] results = model.recommendProducts(r.user(), 6);
			 MysqlUtil.saveUserRecommend(results);
		}
		
		
		
	}

}
