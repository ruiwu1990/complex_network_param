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
import java.io._

// // it will change the array
// def test(input_arr:Array[Int]) : Int = {
// 	input_arr(0) = -10
// 	return 0	
// }

val filename = "static/data/scale_free_power_law.net"
// val filename = "static/data/simple.net"
// val filename = "static/data/nexusanon.net"

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

// get the closeness centrality
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.ShortestPaths

val vertex_num = graph.vertices.collect().length

def closeness_centrality(start:Int,vertex_num:Int) : Float = {
	
	var temp_sum = 0f
	for(count <- 1 to vertex_num ){
		// from start to count
		// if count == start then shortest path is 0
		val result = ShortestPaths.run(graph, Seq(count))
		if(result.vertices.filter({case(vId, _) => vId == start}).first._2.get(count)!=None){
			temp_sum = temp_sum + result.vertices.filter({case(vId, _) => vId == start}).first._2.get(count).get.toFloat
		}
	}
	return ((vertex_num.toFloat-1)/temp_sum)

}

val closeness_cen = closeness_centrality(1,vertex_num)

// shortest path from 1 to 4
// val result = ShortestPaths.run(graph, Seq(4))
// val shortestPath = result.vertices.filter({case(vId, _) => vId == 1}).first._2.get(4)
// val shortestPath = result               // result is a graph
//   .vertices                             // we get the vertices RDD
//   .filter({case(vId, _) => vId == 1})  // we filter to get only the shortest path from v1
//   .first                                // there's only one value
//   ._2                                   // the result is a tuple (v1, Map)
//   .get(160)  



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

def vertex_hop_distribution(vertex_id:Long,result_matrix:Array[Array[Long]]) = {
	var pre_arr = ArrayBuffer[Long]()
	var cur_arr = ArrayBuffer[Long]()
	var new_arr = ArrayBuffer[Long](vertex_id)
	var result_arr = Set[Long]()
	var loop_count = 0
	// stop if there is no new node or there is a loop
	while((new_arr.length!=0) && (pre_arr.toSet!=(pre_arr++new_arr).toSet)){
		// union two arraybuffer
		cur_arr.foreach(i=>
			if((pre_arr contains i)==false)
				pre_arr += i
		)

		cur_arr = new_arr
		new_arr = find_neighbour(new_arr)
		// this record the vertices IDs
		result_arr = pre_arr.toSet.union(cur_arr.toSet)
		// ID starts from 1, array boundary starts from 1, so -1
		result_matrix(vertex_id.toInt-1)(loop_count) = result_arr.toArray.length
		// println("cur loop: "+loop_count)
		// println("cur status: "+result_arr.mkString(","))
		loop_count = loop_count + 1

		// Step2 put results into a file
	}

}

// this function get final normalized hop distribution
def normalized_hop_distribution(result_matrix:Array[Array[Long]],normalized_result:Array[Float],element_num:Int) = {
	for(r <- 0 to (element_num-1)){
		for(c <- 0 to (element_num-1)){
			normalized_result(c) = normalized_result(c) + (result_matrix(r)(c).toFloat/element_num.toFloat)
		}
	}
	for(c <- 0 to (element_num-1)){
		normalized_result(c) = normalized_result(c)/element_num.toFloat
	}
	// fill the rest zero with the previous elements
	// without this the array can be something like this (0.2,0.24,0,0
	var pre = 0.0
	for(c <- 0 to (element_num-1)){
		if(normalized_result(c).toFloat == 0.0){
			normalized_result(c) = pre
		}
		else{
			pre = normalized_result(c)
		}
	}
}

val vertex_num = graph.vertices.collect().length
// the maximum hop should be vertex_num - 1
// create 2D matrix to record the results, avoid writing conflicts
// var hop_distribution_matrix = Array.ofDim[ArrayBuffer[Long]](vertex_num,vertex_num-1)
var hop_distribution_matrix = Array.ofDim[Long](vertex_num,vertex_num)
val temp_vertices_arr = graph.vertices.collect()

// start time 
val t_start = System.currentTimeMillis

// foreach will run by parallel by referring to this link:
// http://stackoverflow.com/questions/38069239/parallelize-avoid-foreach-loop-in-spark
temp_vertices_arr.foreach(i => vertex_hop_distribution(i._1.toLong,hop_distribution_matrix))

var normalized_result = Array.ofDim[Float](vertex_num)
normalized_hop_distribution(hop_distribution_matrix,normalized_result,vertex_num)

val t_end = System.currentTimeMillis
println("total time:"+(t_end-t_start))

val pw = new PrintWriter(new File("result.txt"))
val final_arr = final_result_degree.collect
final_arr.foreach(i=>pw.write(i._1.toString()+","+i._2.toString()+"&&"))
pw.write("\n")
pw.write(closeness_cen.toString())
pw.write("\n")
pw.write(normalized_result.mkString(","))
pw.close
System.exit(0)
