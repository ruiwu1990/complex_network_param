// This file is used to parse net file
// and load it into saprk
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD
import scala.io.Source
import scala.collection.mutable.ArrayBuffer

// // it will change the array
// def test(input_arr:Array[Int]) : Int = {
// 	input_arr(0) = -10
// 	return 0	
// }

val filename = "test.net"

var temp_vertex = Array[String]()
var temp_edge = Array[String]()
var cur_vertex = -1
var temp = Array[String]()
for (line<-Source.fromFile(filename).getLines()){
	// .filter(_.nonEmpty) remove the empty elements in the string
	temp = line.split(" ").filter(_.nonEmpty)
	// temp = temp.filter(_.nonEmpty)
	if(temp(0)=="*Vertices"){
		cur_vertex = 0

	}
	else if(temp(0)=="*Edges"){
		cur_vertex = 1
	}
	else if(cur_vertex == 0){
		// append ID and name
		temp_vertex = temp_vertex:+temp(0):+ temp(1)
	}
	else if(cur_vertex == 1){
		// append start, to, and weight
		temp_edge = temp_edge:+temp(0):+ temp(1):+ temp(2)
	}
}

// parepare 
// var temp_graph_vertex =new  Array[(Int,String)](temp_vertex.length/2)
var temp_graph_vertex = new Array[(Long,String)](temp_vertex.length/2)
var count = 0
for (count<-0 to (temp_vertex.length/2-1)){
	temp_graph_vertex(count) = (temp_vertex(count*2).toLong,temp_vertex(count*2+1))
}

val graph_vertex: RDD[(VertexId,String)] = sc.parallelize(temp_graph_vertex)

// edge contains weight
var temp_graph_edge = new Array[Edge[Int]](temp_edge.length/3)
for (count<-0 to (temp_edge.length/3-1)){
	temp_graph_edge(count) = Edge(temp_edge(count*3).toLong,temp_edge(count*3+1).toLong,temp_edge(count*3+2).toInt)
}

val graph_edge: RDD[Edge[Int]] = sc.parallelize(temp_graph_edge)

val default_node = ("missing_node","missing")
// the function if for get max degree
// return (vetex ID, result)
graph.inDegrees.max
graph.outDegrees.max
graph.degrees.max


println(temp_vertex(0).mkString(" "))
println(temp_vertex(1).mkString(" "))

println(temp_edge(0).mkString(" "))
println(temp_edge(1).mkString(" "))

System.exit(0)

