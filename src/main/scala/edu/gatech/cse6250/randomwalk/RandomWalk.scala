package edu.gatech.cse6250.randomwalk

import edu.gatech.cse6250.model.{ PatientProperty, EdgeProperty, VertexProperty }
import org.apache.spark.graphx._
import org.apache.spark.graphx.TripletFields

object RandomWalk {

  def delta(u: VertexId, v: VertexId): Double = {if (u==v) 1.0 else 0.0}

  def runUpdate(randGraph: Graph[Double, Double], alpha: Double, src: VertexId): Graph[Double, Double] = {
    val randUpdates = randGraph.aggregateMessages[Double](ctx=>ctx.sendToDst(ctx.srcAttr * ctx.attr), _+_, TripletFields.Src)
    val rPrb  = (src: VertexId, id: VertexId)=>alpha * delta(src, id)

    val outGraph = randGraph.outerJoinVertices(randUpdates) {
      (id, oldRank, msgSumOpt) => rPrb(src, id) + (1.0 - alpha) * msgSumOpt.getOrElse(0.0)
    }

    outGraph
  }

  def randomWalkOneVsAll(graph: Graph[VertexProperty, EdgeProperty], patientID: Long, numIter: Int = 100, alpha: Double = 0.15): List[Long] = {
    /**
     * Given a patient ID, compute the random walk probability w.r.t. to all other patients.
     * Return a List of patient IDs ordered by the highest to the lowest similarity.
     * For ties, random order is okay
     */

    /** Remove this placeholder and implement your code */
    // inspiration taken from scala PageRank object
    // initialize edges and vertices
    //val randGraph = graph.outerJoinVertices(graph.outDegrees) {(vid, vdata, deg) => deg.getOrElse(0)}.mapTriplets(e => (1.0 / e.srcAttr, e.attr)).mapVertices{(id, attr)=>if (id==patientID) 1.0 else 0.0}
    var randGraph = graph.outerJoinVertices(graph.outDegrees) {(vid, vdata, deg) => deg.getOrElse(0)}.mapTriplets(e => 1.0 / e.srcAttr, TripletFields.Src).mapVertices{ (id, attr)=>if (id==patientID) 1.0 else 0.0}
    val patient_ids = graph.vertices.filter(x=>x._2.isInstanceOf[PatientProperty]).map(x=>x._1).collect()


    var curIter = 0
    var prevRandGraph: Graph[Double, Double] = null
    while (curIter < numIter) {
      randGraph.cache()
      prevRandGraph = randGraph

      randGraph = runUpdate(randGraph, alpha, patientID.asInstanceOf[VertexId])
      randGraph.cache()

      randGraph.edges.foreachPartition(x=> {})
      prevRandGraph.vertices.unpersist()
      prevRandGraph.edges.unpersist()
      curIter += 1
    }

    val top_patients = randGraph.vertices.sortBy(_._2, false).filter(x=>patient_ids.contains(x._1)).filter(x=>x._1 != patientID).take(10).map(x=>x._1).toList



    top_patients
    //List(1, 2, 3, 4, 5)
  }
}
