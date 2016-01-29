///**
// * Created by Ayou on 2015/11/9.
// */
//object Test {
//  def main(args: Array[String]) {
//    val conf = new SparkConf().setAppName("Log").setMaster("local[2]")
//    val sc = new SparkContext(conf)
//    val sqlContext = new SQLContext(sc)
//    val row = sc.parallelize(List(("1",1))).map(x => Row(x._1,x._2))
//    val schema = StructType(
//      StructField("ip",StringType) ::
//        StructField("count",IntegerType) :: Nil
//    )
//    val prop = new Properties()
//    prop.setProperty("user","root")
//    prop.setProperty("password","123456")
//    val url="jdbc:mysql://localhost:3306/log"
//    val data = sqlContext.createDataFrame(row,schema)
//    data.registerTempTable("ip")
//    sqlContext.sql("INSERT INTO ip values ('1','1')")
////    data.write.jdbc(url,"ip_new",prop)
////    data.sel
//   // sqlContext.c
// //   val jdbcDF = sqlContext.createExternalTable(“jdbc”, Map(“url” -> “jdbc:mysql://localhost:3306/your_database?user=your_user&password=your_password”, “dbtable” -> “your_table”))
//  }
//}
