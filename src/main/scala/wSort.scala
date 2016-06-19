import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 16-6-19.
  */
object wSort {

  def main(args: Array[String]) {
    if (args.length < 4) {
      /*
      四个参数，
      <zk的ip地址和端口号>
      <此程序扮演的是kafka的consumer,所以要填写consumergroup是什么>
      <kafka分布式消息队列，自己在kafka创建的哪个topic传入哪个topic就可以了>
      <线程数量，resever里面起几个线程去消费topic里面的数据>
      */
      System.err.println("Usage: KafkaLog <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }
    /*
    程序扔到集群去执行的时候会打印很多日志，日志会淹没我们主要想看的信息，
    所以将info之类的日志屏蔽掉，只有WARN类型日志才会显示
     */
    Logger.getRootLogger.setLevel(Level.WARN)

    val Array(zkQuorum,gtoup,topics,numThreads) = args //将传入的参数灌到数组里面去
    val conf = new SparkConf()
    //conf.setMaster("local[2]")
    conf.setMaster("spark://HTY-1:7077").setAppName("wordSort").set("spark.executor.memory","256m")

    val ssc = new StreamingContext(conf,Seconds(2)) //把流分为两秒为单位的batch，一小批一小批的batch

    ssc.checkpoint("checkpoint") //为容错和恢复做准备
    //把我们传进来的topic和对应的在resever里面消费topic数据的线程数，展成一个Map
    val topicMap = topics.split(",").map((_,numThreads.toInt)).toMap

    //建立链接这个很重要sparkstreaming 返回的是DStream。一个MAP结构传进来KAFKA
    val lines = KafkaUtils.createStream(ssc,zkQuorum,group,topicMap).map(_._2)

    val keyRegex = """\[[^\]]+]"""
    val keywords = lines.map(line=>keyRegex.r.findAllIn(line).mkString).filter(_!="")
    val wordCount = keywords.map(word=>(word,1)).reduceByKeyAndWindow(_+_,_-_,Minutes(5),Seconds(2))
    val sorted = wordCount.map(word=>(word._2,word._1)).transform(_.sortByKey(false)).map(word=>(word._2,word._1))

    sorted.print()

    scc.start()
    scc.awaitTermination()
  }
}