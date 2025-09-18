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

    // executing rating-based analysis using filter and ordering
    performRatingBasedAnalysis(companiesDataset, s"$outputDirectoryPath/rating-analysis")

    // executing salary analysis using groupBy operations
    performSalaryAnalysis(companiesDataset, s"$outputDirectoryPath/salary-analysis")

    // executing advanced filtering using combinators and closures
    performAdvancedFilteringAnalysis(companiesDataset, s"$outputDirectoryPath/advanced-filtering")

    // executing high-potential companies analysis using join operations
    performHighPotentialAnalysis(companiesDataset, s"$outputDirectoryPath/high-potential-analysis")

    println("\nAll analytics operations completed successfully!")
  }

  /**
   * Pure function to display application header information
   * Provides clear application identification and purpose
   */
  private def printApplicationHeader(): Unit = {
    println("=" * 65)
    println("Functional Data Processing Pipeline with Apache Spark")
    println("Company Analytics System")
    println("=" * 65)
  }

  /**
   * Pure function for basic statistical analysis of companies data
   * Uses functional aggregations and pure computations
   *
   * @param companiesDataset Dataset of companies to analyze
   * @param spark Implicit SparkSession for Spark operations
   */
  private def performBasicStatisticalAnalysis(companiesDataset: Dataset[Company])(implicit spark: SparkSession): Unit = {
    // validating arguments
    require(companiesDataset != null, "Companies dataset cannot be null")

    import spark.implicits._

    println("\nBasic Statistical Analysis:")
    println("-" * 35)

    // performing comprehensive analytics using processor
    val analyticsResults = CompanyProcessor.performComprehensiveAnalytics(companiesDataset)

    // displaying analytics results with proper formatting
    analyticsResults.foreach {
      case ("totalCompaniesCount", count) =>
        println(s"Total Companies Analyzed: $count")
      case ("averageCompanyRating", avgRating: Double) =>
        println(f"Average Company Rating: ${avgRating}%.2f")
      case ("sectorDistributionData", distribution: Map[String, Long]) =>
        println("Sector Distribution:")
        distribution.toSeq.sortBy(-_._2).foreach { case (sector, count) =>
          println(s"  • $sector: $count companies")
        }
      case ("topRatedCompanies", companies: List[(String, Double)]) =>
        println("Top 5 Highest Rated Companies:")
        companies.zipWithIndex.foreach { case ((name, rating), index) =>
          println(f"  ${index + 1}. $name: ${rating}%.2f")
        }
      case ("ratingCategoriesDistribution", categories: Map[String, Long]) =>
        println("Rating Categories Distribution:")
        categories.toSeq.sortBy(-_._2).foreach { case (category, count) =>
          println(s"  • $category: $count companies")
        }
      case _ => // ignoring other entries for basic analysis
    }
  }

  /**
   * Pure function for sector-based analysis using Spark transformations
   * Demonstrates map and aggregation operations for sector insights
   *
   * @param companiesDataset Dataset of companies to analyze
   * @param outputPath File path for saving results
   * @param spark Implicit SparkSession for operations
   */
  private def performSectorBasedAnalysis(companiesDataset: Dataset[Company], outputPath: String)
                                        (implicit spark: SparkSession): Unit = {
    // validating arguments
    require(companiesDataset != null, "Companies dataset cannot be null")
    require(outputPath != null && outputPath.trim.nonEmpty, "Output path cannot be null or empty")

    println("\nSector-Based Analysis:")
    println("-" * 25)

    // analyzing companies by sector using processor functions
    val sectorCountsData = CompanyProcessor.companiesBySector(companiesDataset)
    val sectorSalaryAverages = CompanyProcessor.averageSalaryBySector(companiesDataset)

    println("Companies Count by Sector:")
    sectorCountsData.collect().sortBy(-_._2).take(5).foreach { case (sector, count) =>
      println(s"  • $sector: $count companies")
    }

    println("\nAverage Salary by Sector:")
    sectorSalaryAverages.collect().sortBy(-_._2).take(5).foreach { case (sector, avgSalary) =>
      println(f"  • $sector: ${avgSalary}%.2f")
    }

    // saving results using functional I/O handling
    CompanyProcessor.saveAnalysisResults(sectorCountsData, s"$outputPath/sector-counts") match {
      case Success(_) => println(s"Sector analysis results saved to $outputPath/sector-counts")
      case Failure(exception) => println(s"Failed to save sector analysis: ${exception.getMessage}")
    }
  }

  /**
   * Pure function for rating-based analysis using filter and ordering
   * Demonstrates functional data filtering and sorting operations
   *
   * @param companiesDataset Dataset of companies to analyze
   * @param outputPath File path for saving results
   * @param spark Implicit SparkSession for operations
   */
  private def performRatingBasedAnalysis(companiesDataset: Dataset[Company], outputPath: String)
                                        (implicit spark: SparkSession): Unit = {
    // validating arguments
    require(companiesDataset != null, "Companies dataset cannot be null")
    require(outputPath != null && outputPath.trim.nonEmpty, "Output path cannot be null or empty")

    println("\nRating-Based Analysis:")
    println("-" * 23)

    // finding top-rated companies using processor functions
    val topRatedCompanies = CompanyProcessor.topCompaniesByRating(companiesDataset, 10)

    println("Top 10 Highest Rated Companies:")
    topRatedCompanies.collect().zipWithIndex.foreach { case ((name, rating), index) =>
      println(f"  ${index + 1}. $name: ${rating}%.2f")
    }

    // saving results with error handling
    CompanyProcessor.saveAnalysisResults(topRatedCompanies, outputPath) match {
      case Success(_) => println(s"Rating analysis results saved to $outputPath")
      case Failure(exception) => println(s"Failed to save rating analysis: ${exception.getMessage}")
    }
  }

  /**
   * Pure function for salary analysis using pattern matching and grouping
   * Demonstrates functional categorization and statistical analysis
   *
   * @param companiesDataset Dataset of companies to analyze
   * @param outputPath File path for saving results
   * @param spark Implicit SparkSession for operations
   */
  private def performSalaryAnalysis(companiesDataset: Dataset[Company], outputPath: String)
                                   (implicit spark: SparkSession): Unit = {
    // validating arguments
    require(companiesDataset != null, "Companies dataset cannot be null")
    require(outputPath != null && outputPath.trim.nonEmpty, "Output path cannot be null or empty")

    import spark.implicits._

    println("\nSalary Analysis:")
    println("-" * 18)

    // categorizing companies by salary ranges
    val salaryCategories = companiesDataset
      .map(_.salaryScore)
      .groupByKey(identity)
      .count()
      .orderBy($"value".desc)

    println("Companies by Salary Categories:")
    salaryCategories.collect().foreach { case (category, count) =>
      println(s"  • $category: $count companies")
    }

    // calculating industry benchmarks
    val industryBenchmarks = CompanyProcessor.calculateIndustryBenchmarks(companiesDataset)
    println("\nIndustry Benchmarks:")
    industryBenchmarks.foreach { case (metric, value) =>
      println(f"  • $metric: ${value}%.2f")
    }

    // saving results with functional error handling
    CompanyProcessor.saveAnalysisResults(salaryCategories, outputPath) match {
      case Success(_) => println(s"Salary analysis results saved to $outputPath")
      case Failure(exception) => println(s"Failed to save salary analysis: ${exception.getMessage}")
    }
  }

  /**
   * Pure function demonstrating advanced filtering with combinators
   * Shows functional composition and currying techniques in action
   *
   * @param companiesDataset Dataset of companies to analyze
   * @param outputPath File path for saving results
   * @param spark Implicit SparkSession for operations
   */
  private def performAdvancedFilteringAnalysis(companiesDataset: Dataset[Company], outputPath: String)
                                              (implicit spark: SparkSession): Unit = {
    // validating arguments
    require(companiesDataset != null, "Companies dataset cannot be null")
    require(outputPath != null && outputPath.trim.nonEmpty, "Output path cannot be null or empty")

    println("\nAdvanced Filtering Analysis (Combinators & Currying):")
    println("-" * 52)

    // example 1: Technology companies with high ratings
    val techHighRated = CompanyProcessor.advancedFilteringWithCombinators(
      companiesDataset, 4.0, "Technology")
    println(s"High-rated Technology companies: ${techHighRated.count()}")

    // example 2: Using curried functions directly for Financial Services
    val financialServicesFilter = Company.filterBySector("Financial Services")
    val highRatingFilter = Company.filterByMinRating(3.5)

    val financialHighRated = companiesDataset.filter(company =>
      financialServicesFilter(company) && highRatingFilter(company)
    )
    println(s"High-rated Financial Services companies: ${financialHighRated.count()}")

    // saving filtered results with error handling
    CompanyProcessor.saveAnalysisResults(techHighRated, outputPath) match {
      case Success(_) => println(s"Advanced filtering results saved to $outputPath")
      case Failure(exception) => println(s"Failed to save filtering results: ${exception.getMessage}")
    }
  }

  /**
   * Pure function for high-potential company analysis using joins
   * Demonstrates join operations and functional composition techniques
   *
   * @param companiesDataset Dataset of companies to analyze
   * @param outputPath File path for saving results
   * @param spark Implicit SparkSession for operations
   */
  private def performHighPotentialAnalysis(companiesDataset: Dataset[Company], outputPath: String)
                                          (implicit spark: SparkSession): Unit = {
    // validating arguments
    require(companiesDataset != null, "Companies dataset cannot be null")
    require(outputPath != null && outputPath.trim.nonEmpty, "Output path cannot be null or empty")

    println("\nHigh-Potential Companies Analysis (Join Operations):")
    println("-" * 54)

    // finding high-potential companies using processor functions
    val highPotentialCompanies = CompanyProcessor.findHighPotentialCompanies(companiesDataset)
    val highPotentialCount = highPotentialCompanies.count()

    println(s"High-potential companies identified: $highPotentialCount")

    if (highPotentialCount > 0) {
      println("Sample high-potential companies:")
      highPotentialCompanies.take(5).foreach { company =>
        println(f"  • ${company.companyName} (${company.extractSector}): ${company.companyRatings}%.2f rating")
      }

      // saving results with functional error handling
      CompanyProcessor.saveAnalysisResults(highPotentialCompanies, outputPath) match {
        case Success(_) => println(s"High-potential analysis results saved to $outputPath")
        case Failure(exception) => println(s"Failed to save high-potential analysis: ${exception.getMessage}")
      }
    } else {
      println("No high-potential companies found with current criteria")
    }
  }
}