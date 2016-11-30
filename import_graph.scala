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
// var temp_graph_vertex = new Array[(Long,String)](temp_vertex.length/2)
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


val graph = Graph(graph_vertex,graph_edge)
// the function, get max degree
// http://spark.apache.org/docs/latest/graphx-programming-guide.html
def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
  if (a._2 > b._2) a else b
}
val max_in_degree: (VertexId, Int)  = graph.inDegrees.reduce(max)
val max_out_degree: (VertexId, Int) = graph.outDegrees.reduce(max)
val max_degrees: (VertexId, Int)   = graph.degrees.reduce(max)

// get the degree info
val in_degrees: VertexRDD[Int] = graph.inDegrees
val out_degrees: VertexRDD[Int] = graph.outDegrees
val degrees: VertexRDD[Int] = graph.degrees

// get degree distribution
var count = 0
var temp_degree_pair = degrees.map(line => (line._2,1))
val result_degree = temp_degree_pair.groupByKey()
val final_result_degree = result_degree.map(line => (line._1,line._2.sum))

/*
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.ShortestPaths

val result = ShortestPaths.run(graph, Seq(160))
val shortestPath = result               // result is a graph
  .vertices                             // we get the vertices RDD
  .filter({case(vId, _) => vId == 1})  // we filter to get only the shortest path from v1
  .first                                // there's only one value
  ._2                                   // the result is a tuple (v1, Map)
  .get(160)  
*/
// val shortestPath = result.vertices.filter({case(vId, _) => vId == 160}).first._2.get(1)

import scala.collection.mutable.ArrayBuffer
// this code find all the neighbours of the vertex
// return the merge
def find_neighbour(new_arr:ArrayBuffer[Long]) : ArrayBuffer[Long] = {
	var final_result = ArrayBuffer[Long]()
	for(item <- new_arr){
		// extract neighbours based on srcId
		val temp = graph.edges.filter(e => e.srcId == item)
		val temp_RDD = temp.map(e=>e.dstId)
		val temp_RDD_long = temp_RDD.map(e=>e.toLong)
		// collect convert RDD into array
		final_result = final_result ++ temp_RDD_long.collect()
	}
	return final_result
}
// val temp = graph.edges.filter(e => e.srcId == 1)


// this function is used to get the hop distribution of a single node
import scala.collection.mutable.ArrayBuffer

def vertex_hop_distribution(vertex_id:Long) = {
	var pre_arr = ArrayBuffer[Long]()
	var cur_arr = ArrayBuffer[Long]()
	var new_arr = ArrayBuffer[Long](vertex_id)
	var loop_count = 0

	while(new_arr.length != 0){
		println("cur loop: "+loop_count)
		println("cur new: "+new_arr.mkString(","))
		loop_count = loop_count + 1
		pre_arr = cur_arr
		cur_arr = new_arr
		new_arr = find_neighbour(new_arr)
		// Step2 put results into a file
	}

}

System.exit(0)
