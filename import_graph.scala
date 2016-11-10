// This file is used to parse net file
// and load it into saprk
import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD
import scala.io.Source

// // it will change the array
// def test(input_arr:Array[Int]) : Int = {
// 	input_arr(0) = -10
// 	return 0	
// }

val filename = "test.net"

var temp_vertex = Array[String]()
var temp_edge = Array[String]()
var cur_vertex = 0
var temp = Array[String]()
for (line<-Source.fromFile(filename).getLines()){
	// .filter(_.nonEmpty) remove the empty elements in the string
	temp = line.split(" ").filter(_.nonEmpty)
	// temp = temp.filter(_.nonEmpty)
	if(temp(0)=="*Vertices"){
		cur_vertex = 0

	}
	else if(temp(0)!="*Vertices" && cur_vertex == 0){
		// append ID and name
		temp_vertex = temp_vertex:+temp(0):+temp(1)
	}
	else if(temp(0)=="*Edges" && cur_vertex == 0){
		cur_vertex = 1
	}
	else if(temp(0)!="*Edges" && cur_vertex == 1){
		// append start, to, and weight
		temp_edge = temp_edge:+temp(0):+temp(1):+temp(2)
	}
}

println(temp_vertex(0).mkString(" "))
println(temp_vertex(1).mkString(" "))

println(temp_edge(0).mkString(" "))
println(temp_edge(1).mkString(" "))

System.exit(0)

