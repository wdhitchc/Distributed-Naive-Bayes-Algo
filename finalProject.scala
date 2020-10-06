import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import scala.collection.mutable
import scala.collection.mutable._


object finalProject {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("finalProject").setMaster("local[4]")

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sc = new SparkContext(conf)


    val train = sc.textFile("train.csv")
    // val test = sc.textFile("bayesTest.txt")

    // scale array of Doubles in string format
    def scaleData(nums: Array[String])   = {
      val newnums = nums.map(x => x.toDouble)
      val numsavg = newnums.sum / newnums.length

      val stdDev = Math.sqrt((newnums.map( _ - numsavg)
        .map(t => t*t).sum)/newnums.length)

      (numsavg, stdDev)
    }

    // encode Array of scaled number in string format by there quantile
    def quantileAssign(nums: Array[(String, String)]) : Array[String] = {

      val index = (nums.length / 5.0).toInt
      return(Array[String](index.toString, (2 * index).toString, ( 3* index).toString, (4* index).toString))

    }


    def quantileEncode(data : Array[(String, String)], sorted : Array[Double], breaks: Array[String]) = {
        data.map(x => (x._1, x._2.toDouble)).map(x =>
          if (x._2 <= sorted(breaks(0).toInt)){
            (x._1, "1")
          }
          else if( x._2 <= sorted(breaks(1).toInt)){
            (x._1, "2")
          }
          else if ( x._2 <= sorted(breaks(2).toInt)){
            (x._1, "3")
          }
          else if ( x._2 <= sorted(breaks(3).toInt)){
            (x._1, "4")
          }
          else{
            (x._1, "5")
          }
        )
    }


    def getRowbyIndex(data :RDD[Array[String]], lastContCol : Int, lastCol : Int) = {

      var col1 = data.map(x => (x(0), x(1))).collect()
      var MeanStd = scaleData(col1.map(x=> x._2))

      col1 = col1.map(x => (x._1, (x._2.toDouble - MeanStd._1 / MeanStd._2).toString()))
      var breaks = quantileAssign(col1)
      var sorted = col1.map(x => x._2.toDouble).sortWith((x,y) => x < y)

      col1 = quantileEncode(col1, sorted, breaks)

      // for the continous columns
      for(i <- 2 to lastContCol ){
        var coli = data.map(x => (x(0), x(i))).collect()
        MeanStd = scaleData(coli.map(x => x._2.toString))
        coli = coli.map(x => (x._1, (x._2.toDouble - MeanStd._1 / MeanStd._2).toString))

        var breaks = quantileAssign(coli)
        var sorted = coli.map(x => x._2.toDouble).sortWith((x,y) => x < y)
        coli = quantileEncode(coli, sorted, breaks)


        col1 = sc.parallelize(col1)
          .join(sc.parallelize(coli))
          .collect()
          .map(x => (x._1, x._2._1 + "," + x._2._2))
      }

      for(i <- lastContCol + 1 to lastCol){
        var coli = data.map(x => (x(0), x(i))).collect()
        col1 = sc.parallelize(col1)
          .join(sc.parallelize(coli))
            .collect().map(x => (x._1, x._2._1 + "," + x._2._2))
      }

      col1
    }

    val traintoProb = train.map(line => line.split(","))
    val RowbyIndex = getRowbyIndex(traintoProb, 10, 18)
    val rows = sc.parallelize(RowbyIndex.sortWith((x,y) => x._1.toDouble > y._1.toDouble))
    val newRows = rows.map(x => (x._1, x._2.split(",")))


    //gives P(B | A)
    def getConditionalGiven(col:Int, quant:Int, revCol:Int, revcolMatch:Int) : Double =  {

      (newRows
        .filter(x => x._2(col) == quant.toString & x._2(revCol) == revcolMatch.toString)
        .collect().length.toDouble) /
        (newRows
          .filter(x => x._2(revCol) == revcolMatch.toString)
          .collect().length.toDouble)
    }

    //returns all conditional Probs as a hashmap
    def learnBayes(contCol : Int) = {

      var res = new mutable.HashMap[(Int, Int),Double]()
      val revcol = 17
      //for all the continous columns (Now mapped to quantiles...)
      for (col <- 0 to contCol) {
        for (quant <- 1 to 5) {
            res.put((col, quant), getConditionalGiven(col, quant, revcol, 1))
        }
      }
      for(col <- contCol + 1 to revcol - 1){
       for ( x <- newRows.map(x => x._2(col)).collect().distinct) {
         res.put((col, x.toInt), getConditionalGiven(col, x.toInt, revcol, 1))
       }
      }
      res
    }

    val probs = learnBayes(10)


    def probRow(tup : (String, Array[String]) ) = {

      var res = 1.0
      for (i <- 0 to 16) {
        res = res * probs((i, tup._2(i).toInt))
      }
      res
    }


    def predictBayes(data : RDD[Array[String]], probs : mutable.HashMap[(Int, Int), Double]) = {

      val predictRowbyIndex = getRowbyIndex(data, 10, 18)
      val predictrows = sc.parallelize(predictRowbyIndex.sortWith((x,y) => x._1.toDouble > y._1.toDouble))
      val predictnewRows = predictrows.map(x => (x._1, x._2.split(",")))
      println(predictnewRows == newRows)

      //val probRev
      //val probNot REv

    }



    predictBayes(train.map(line => line.split(",")), probs)




  }
}
