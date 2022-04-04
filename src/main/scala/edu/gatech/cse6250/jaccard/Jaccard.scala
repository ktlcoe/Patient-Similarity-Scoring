/**
 *
 * students: please put your implementation in this file!
 */
package edu.gatech.cse6250.jaccard

import edu.gatech.cse6250.model._
import edu.gatech.cse6250.model.{ EdgeProperty, VertexProperty }
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object Jaccard {

  def jaccardSimilarityOneVsAll(graph: Graph[VertexProperty, EdgeProperty], patientID: Long): List[Long] = {
    /**
     * Given a patient ID, compute the Jaccard similarity w.r.t. to all other patients.
     * Return a List of top 10 patient IDs ordered by the highest to the lowest similarity.
     * For ties, random order is okay. The given patientID should be excluded from the result.
     */

    /** Remove this placeholder and implement your code */
    val direction: EdgeDirection = EdgeDirection.Out
    val neighbors = graph.collectNeighborIds(direction).filter(x=>x._1==patientID).map(x=>x._2.mkString(",")).collect()
    //val neighbor_set = neighbors.map(_.toLong)

    val other_patient_ids = graph.vertices.filter(x=>x._2.isInstanceOf[PatientProperty]).filter(x=>x._1 != patientID).map(x=>x._1).collect()
    val other_neighbors = graph.collectNeighborIds(direction).filter(x=>other_patient_ids.contains(x._1)).map(x=>(x._1, x._2.mkString(",")))
    val jaccard_scores = other_neighbors.map{x=>
      val other_set = x._2.split(",").toSet
      val coeff = jaccard(neighbors.toSet, other_set)
      (patientID, x._1, coeff)
    }.sortBy(_._3, false).take(10).map(x=>x._2.toLong).toList

    jaccard_scores
    //val trip = graph.triplets
    //val cur_patient = trip.filter(x=>x.srcId==patientID)
    //val cur_set = Set(cur_patient.map(x=>x.dstAttr).collect())
    //List(1, 2, 3, 4, 5)
  }

  def jaccardSimilarityAllPatients(graph: Graph[VertexProperty, EdgeProperty]): RDD[(Long, Long, Double)] = {
    /**
     * Given a patient, med, diag, lab graph, calculate pairwise similarity between all
     * patients. Return a RDD of (patient-1-id, patient-2-id, similarity) where
     * patient-1-id < patient-2-id to avoid duplications
     */
    /** Remove this placeholder and implement your code */
    val sc = graph.edges.sparkContext
    val direction: EdgeDirection = EdgeDirection.Out
    val all_neighbors = graph.collectNeighborIds(direction).map(x=>(x._1, x._2.mkString("/")))
    val patient_ids = graph.vertices.filter(x=>x._2.isInstanceOf[PatientProperty]).map(x=>x._1).collect()
    val all_neighbors_patients = all_neighbors.filter(x=>patient_ids.contains(x._1))
    val all_jaccards = all_neighbors_patients.cartesian(all_neighbors_patients).filter(x=>x._1._1<x._2._1).map{x=>
      val set1 = x._1._2.split("/").toSet
      val set2 = x._2._2.split("/").toSet
      val coeff = jaccard(set1, set2)
      (x._1._1, x._2._1, coeff)
    }

    all_jaccards
  }

  def jaccard[A](a: Set[A], b: Set[A]): Double = {
    /**
     * Helper function
     *
     * Given two sets, compute its Jaccard similarity and return its result.
     * If the union part is zero, then return 0.
     */

    /** Remove this placeholder and implement your code */
    val set_intersect = a & b
    val set_union = a | b
    val coeff = if (set_union.size==0) {
      0 } else {
      set_intersect.size*1.0 / set_union.size
    }
    coeff
  }
}
