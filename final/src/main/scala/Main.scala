package il.ac.hit.finalproject

import il.ac.hit.finalproject.model.Company
import il.ac.hit.finalproject.processor.CompanyProcessor
import org.apache.spark.sql.{Dataset, SparkSession}
import scala.util.{Success, Failure}

/**
 * Main application entry point for the Functional Data Processing Pipeline.
 * Demonstrates separation of pure logic from I/O operations.
 * Orchestrates the entire data processing workflow.
 *
 * @author [Team Manager Name]
 */
object Main {

  /**
   * Main method - entry point of the application
   * Handles I/O operations and delegates pure computations to processor
   * Follows functional programming principles for error handling
   *
   * @param args Command line arguments
   */
  def main(args: Array[String]): Unit = {
    // validating arguments
    require(args != null, "Arguments array cannot be null")

    // creating Spark session with proper configuration
    implicit val spark: SparkSession = SparkSession
      .builder()
      .appName("Functional Company Analytics Pipeline")
      .master("local[*]")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .getOrCreate()

    // setting log level to reduce console noise
    spark.sparkContext.setLogLevel("WARN")

    // displaying application header
    printApplicationHeader()

    // defining input and output file paths
    val csvInputPath = "data/companies.csv"
    val outputDirectoryPath = "data/output"

    // loading and processing data using functional error handling
    CompanyProcessor.safeCsvLoad(spark, csvInputPath) match {
      case Success(companiesDataset) =>
        println(s"Successfully loaded companies data from $csvInputPath")
        println(s"Total companies loaded: ${companiesDataset.count()}")

        // executing comprehensive analytics workflow
        runComprehensiveAnalyticsWorkflow(companiesDataset, outputDirectoryPath)

      case Failure(exception) =>
        println(s"Failed to load data from $csvInputPath")
        println(s"Error details: ${exception.getMessage}")
        exception.printStackTrace()
    }

    // performing clean shutdown of Spark session
    spark.stop()
    println("Application completed successfully!")
  }

  /**
   * Pure function orchestrating all analytics operations
   * Demonstrates functional composition and modular design
   * Separates business logic from I/O operations
   *
   * @param companiesDataset Dataset of companies to analyze
   * @param outputDirectoryPath Output directory for results
   * @param spark Implicit SparkSession for Spark operations
   */
  private def runComprehensiveAnalyticsWorkflow(companiesDataset: Dataset[Company], outputDirectoryPath: String)
                                               (implicit spark: SparkSession): Unit = {
    // validating arguments
    require(companiesDataset != null, "Companies dataset cannot be null")
    require(outputDirectoryPath != null && outputDirectoryPath.trim.nonEmpty, "Output directory path cannot be null or empty")

    println("\nPerforming comprehensive analytics operations...")

    // executing basic statistical analysis
    performBasicStatisticalAnalysis(companiesDataset)

    // executing sector-based analysis using map and reduceByKey
    performSectorBasedAnalysis(companiesDataset, s"$outputDirectoryPath/sector-analysis")
    