/**
 * @author Hang Su <hangsu@gatech.edu>.
 */

package edu.gatech.cse6250.graphconstruct

import edu.gatech.cse6250.model._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object GraphLoader {
  /**
   * Generate Bipartite Graph using RDDs
   *
   * @input: RDDs for Patient, LabResult, Medication, and Diagnostic
   * @return: Constructed Graph
   *
   */
  def load(patients: RDD[PatientProperty], labResults: RDD[LabResult],
    medications: RDD[Medication], diagnostics: RDD[Diagnostic]): Graph[VertexProperty, EdgeProperty] = {

    val sc = patients.sparkContext

    /** HINT: See Example of Making Patient Vertices Below */
    val vertexPatient: RDD[(VertexId, PatientProperty)] = patients
      .map(patient => (patient.patientID.toLong, PatientProperty(patient.patientID, patient.sex, patient.dob, patient.dod)))
    val patient2VertexId = vertexPatient.map(x=> (x._2.patientID, x._1)).collect.toMap
    val startIndex = patient2VertexId.size
    val vertexDiag: RDD[(VertexId, DiagnosticProperty)] = diagnostics.map(x=>(x.icd9code)).distinct().zipWithIndex().map(x => (x._2+startIndex+1, DiagnosticProperty(x._1)))
    val diag2VertexID = vertexDiag.map(x=>(x._2.icd9code, x._1)).collect.toMap
    val startIndex2 = startIndex + diag2VertexID.size
    val vertexLab: RDD[(VertexId, LabResultProperty)] = labResults.map(x=>x.labName).distinct().zipWithIndex().map(x => (x._2+startIndex2+1, LabResultProperty(x._1)))
    val lab2VertexID = vertexLab.map(x=>(x._2.testName, x._1)).collect.toMap
    val startIndex3 = startIndex2 + lab2VertexID.size
    val vertexMed: RDD[(VertexId, MedicationProperty)] = medications.map(x=>x.medicine).distinct().zipWithIndex().map(x => (x._2+startIndex3+1, MedicationProperty(x._1)))
    val med2VertexID = vertexMed.map(x=>(x._2.medicine, x._1)).collect.toMap

    val vertexPatient0 = vertexPatient.map(x=>(x._1, x._2.asInstanceOf[VertexProperty]))
    val vertexDiag0 = vertexDiag.map(x=>(x._1, x._2.asInstanceOf[VertexProperty]))
    val vertexLab0 = vertexLab.map(x=>(x._1, x._2.asInstanceOf[VertexProperty]))
    val vertexMed0 = vertexMed.map(x=>(x._1, x._2.asInstanceOf[VertexProperty]))
    val vertices = sc.union(vertexPatient0, vertexDiag0, vertexLab0, vertexMed0)


    /**
     * HINT: See Example of Making PatientPatient Edges Below
     *
     * This is just sample edges to give you an example.
     * You can remove this PatientPatient edges and make edges you really need
     */
    val scPatient2VertexId = sc.broadcast(patient2VertexId)
    val scDiag2VertexId = sc.broadcast(diag2VertexID)
    val scLab2VertexId = sc.broadcast(lab2VertexID)
    val scMed2VertexId = sc.broadcast(med2VertexID)

    val labResultsMax = labResults.map(x=>((x.patientID, x.labName), (x.date, x.value))).reduceByKey({case((accDate, accValue), (date, value)) => (math.max(accDate, date), if (accDate>date){accValue} else{value})}).map(x=>(x._1._1, x._2._1, x._1._2, x._2._2))
    val edgePatientLab: RDD[Edge[EdgeProperty]] = labResultsMax.map{
      case(patientId, date, labName, value)=>Edge(scPatient2VertexId.value(patientId), scLab2VertexId.value(labName), PatientLabEdgeProperty(LabResult(patientId, date, labName, value)).asInstanceOf[EdgeProperty])
    }
    val edgeLabPatient: RDD[Edge[EdgeProperty]] = labResultsMax.map{
      case(patientId, date, labName, value)=>Edge(scLab2VertexId.value(labName), scPatient2VertexId.value(patientId),  PatientLabEdgeProperty(LabResult(patientId, date, labName, value)).asInstanceOf[EdgeProperty])
    }

    val diagMax = diagnostics.map(x=>((x.patientID, x.icd9code), (x.date, x.sequence))).reduceByKey({case((accDate, accSeq), (date, seq)) => (math.max(accDate, date), if (accDate>date){accSeq} else{seq})}).map(x=>(Diagnostic(x._1._1, x._2._1, x._1._2, x._2._2)))
    val edgePatientDiag: RDD[Edge[EdgeProperty]] = diagMax.map{
     x=>Edge(scPatient2VertexId.value(x.patientID), scDiag2VertexId.value(x.icd9code), PatientDiagnosticEdgeProperty(x).asInstanceOf[EdgeProperty])
    }
    val edgeDiagPatient: RDD[Edge[EdgeProperty]] = diagMax.map{
      x=>Edge(scDiag2VertexId.value(x.icd9code), scPatient2VertexId.value(x.patientID), PatientDiagnosticEdgeProperty(x).asInstanceOf[EdgeProperty])
    }

    val medMax = medications.map(x=>((x.patientID, x.medicine), x.date)).reduceByKey(math.max).map(x=>Medication(x._1._1, x._2, x._1._2))
    val edgePatientMed: RDD[Edge[EdgeProperty]] = medMax.map{
      x=>Edge(scPatient2VertexId.value(x.patientID), scMed2VertexId.value(x.medicine), PatientMedicationEdgeProperty(x).asInstanceOf[EdgeProperty])
    }
    val edgeMedPatient: RDD[Edge[EdgeProperty]] = medMax.map{
      x=>Edge(scMed2VertexId.value(x.medicine), scPatient2VertexId.value(x.patientID), PatientMedicationEdgeProperty(x).asInstanceOf[EdgeProperty])
    }

    val edges = sc.union(edgePatientLab, edgePatientDiag, edgePatientMed, edgeLabPatient, edgeDiagPatient, edgeMedPatient)


    //val vertexPatient2: RDD[(VertexId, VertexProperty)] = patients
      //.map(patient => (patient.patientID.toLong, PatientProperty(patient.patientID, patient.sex, patient.dob, patient.dod)))
    // Making Graph
    val graph: Graph[VertexProperty, EdgeProperty] = Graph[VertexProperty, EdgeProperty](vertices, edges)
    graph
  }
}
