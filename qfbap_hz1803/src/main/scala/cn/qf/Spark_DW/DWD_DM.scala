package cn.qf.Spark_DW

import cn.qf.Constants.Constan
import cn.qf.SparkUtils.JDBCUtils
import cn.qf.config.ConfigManager
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.LoggerFactory


object DWD_DM {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(Constan.SPARK_APP_NAME_USER).master(Constan.SPARK_LOCAL).enableHiveSupport().getOrCreate()
    val sql = ConfigManager.getProper(args(0))
    if(sql == null) {
      LoggerFactory.getLogger("SparkLogger")
        .debug("提交的表名参数有问题，请重新设置！！！！")
    } else {
      val df = spark.sql(sql)
      val mysqlTableName = args(0).split("\\.")(1)
      val hiveTableName = args(0)
      val jdbcProp = JDBCUtils.getJdbcProp()._1
      val jdbcUrl = JDBCUtils.getJdbcProp()._2
      df.write.mode("append").jdbc(jdbcUrl,mysqlTableName,jdbcProp)
      //df.write.mode(SaveMode.Overwrite).insertInto(hiveTableName)
    }
  }
}
