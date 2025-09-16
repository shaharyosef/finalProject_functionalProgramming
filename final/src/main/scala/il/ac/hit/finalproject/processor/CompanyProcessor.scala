package il.ac.hit.finalproject.processor

import il.ac.hit.finalproject.model.Company
import il.ac.hit.finalproject.analytics.CompanyAnalytics
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.rdd.RDD
import scala.util.{Try, Success, Failure}
import scala.annotation.tailrec

/**
 * Main processor object implementing company data analytics using Apache Spark.
 * Follows functional programming principles with pure functions and immutable data structures.
 * Implements CompanyAnalytics trait for analytical operations.
 */
object CompanyProcessor extends CompanyAnalytics {

  /**
   * Pure function with functional error handling to load CSV data safely
   * Demonstrates functional error handling with Try monad
   * Separates I/O operations from pure logic
   *
   * @param spark SparkSession instance
   * @param filePath CSV file path to load
   * @return Try containing Dataset of companies or failure
   */
  def safeCsvLoad(spark: SparkSession, filePath: String): Try[Dataset[Company]] = {
    // validating arguments
    require(spark != null, "SparkSession cannot be null")
    require(filePath != null && filePath.trim.nonEmpty, "File path cannot be null or empty")

    import spark.implicits._

    Try {
      // reading raw text file using Spark RDD API
      val rawDataRdd = spark.sparkContext.textFile(filePath)
      val headerRow = rawDataRdd.first()

      // filtering out header and malformed rows
      val dataRowsRdd = rawDataRdd.filter(_ != headerRow)

      // transforming CSV rows to Company objects using map operation
      val companiesRdd: RDD[Company] = dataRowsRdd
        .map(_.split(","))
        .filter(_.length >= 10) // filtering malformed rows
        .map(Company.fromCsvRow)

      // converting RDD to Dataset for better type safety
      companiesRdd.toDS()
    }
  }

  /**
   * Pure function using Spark transformations to count companies by sector
   * Uses map and aggregation operations for data analysis
   *
   * @param companiesDataset Dataset of companies to analyze
   * @return Dataset of sector-count pairs
   */
  def companiesBySector(companiesDataset: Dataset[Company]): Dataset[(String, Long)] = {
    // validating arguments
    require(companiesDataset != null, "Companies dataset cannot be null")

    import companiesDataset.sparkSession.implicits._

    // mapping companies to sectors and counting occurrences
    companiesDataset
      .map(company => categorizeSector(company))
      .groupByKey(identity)
      .count()
      .as[(String, Long)]
  }

  /**
   * Pure function to find top N companies by rating using filter and ordering
   * Demonstrates Spark filter, map, and ordering operations
   *
   * @param companiesDataset Dataset of companies
   * @param topN Number of top companies to return
   * @return Dataset of company name and rating pairs
   */
  def topCompaniesByRating(companiesDataset: Dataset[Company], topN: Int = 10): Dataset[(String, Double)] = {
    // validating arguments
    require(companiesDataset != null, "Companies dataset cannot be null")
    require(topN > 0, "Number of top companies must be positive")

    import companiesDataset.sparkSession.implicits._

    // filtering companies with valid ratings and selecting top performers
    companiesDataset
      .filter(_.companyRatings > 0)
      .map(company => (company.companyName, company.companyRatings))
      .orderBy($"_2".desc)
      .limit(topN)
  }

  /**
   * Pure function to calculate average salary by sector using groupBy aggregation
   * Demonstrates Spark groupBy and aggregation operations
   *
   * @param companiesDataset Dataset of companies
   * @return Dataset of sector and average salary pairs
   */
  def averageSalaryBySector(companiesDataset: Dataset[Company]): Dataset[(String, Double)] = {
    // validating arguments
    require(companiesDataset != null, "Companies dataset cannot be null")

    import companiesDataset.sparkSession.implicits._

    // calculating average salary per sector using aggregation
    companiesDataset
      .filter(_.avgSalary > 0)
      .map(company => (categorizeSector(company), company.avgSalary))
      .groupByKey(_._1)
      .agg(typed.avg(_._2))
      .map { case (sector, avgSalary) => (sector, avgSalary) }
      .as[(String, Double)]
  }

  /**
   * Pure function using join operation between companies and sector averages
   * Demonstrates Spark join functionality for data enrichment
   *
   * @param companiesDataset Dataset of companies
   * @param sectorAveragesDataset Dataset of sector averages
   * @return Dataset of companies enriched with sector average data
   */
  def companiesWithSectorAverage(companiesDataset: Dataset[Company],
                                 sectorAveragesDataset: Dataset[(String, Double)]): Dataset[(Company, Double)] = {
    // validating arguments
    require(companiesDataset != null, "Companies dataset cannot be null")
    require(sectorAveragesDataset != null, "Sector averages dataset cannot be null")

    import companiesDataset.sparkSession.implicits._

    // preparing companies with sector information for join
    val companiesWithSectorData = companiesDataset.map(company => (categorizeSector(company), company))

    // performing join operation between companies and sector averages
    companiesWithSectorData
      .joinWith(sectorAveragesDataset, companiesWithSectorData("_1") === sectorAveragesDataset("_1"))
      .map { case ((_, company), (_, sectorAverage)) => (company, sectorAverage) }
  }

  /**
   * Pure function to identify high-potential companies using higher-order functions
   * Uses flatMap and filter operations with closures
   *
   * @param companiesDataset Dataset of companies to analyze
   * @return Dataset of high-potential companies
   */
  def findHighPotentialCompanies(companiesDataset: Dataset[Company]): Dataset[Company] = {
    // validating arguments
    require(companiesDataset != null, "Companies dataset cannot be null")

    val sectorAveragesDataset = averageSalaryBySector(companiesDataset)
    val companiesWithAverageData = companiesWithSectorAverage(companiesDataset, sectorAveragesDataset)

    // using closure to capture the high potential criteria
    companiesWithAverageData
      .filter { case (company, sectorAverageSalary) =>
        company.companyRatings >= 4.0 &&
          company.avgSalary >= sectorAverageSalary * 0.9 &&
          calculatePopularityScore(company) >= 60
      }
      .map(_._1)
  }

  /**
   * Tail-recursive function to calculate compound growth rate
   * Demonstrates tail recursion optimization technique
   *
   * @param salaryData List of historical salary data
   * @param periods Number of time periods
   * @param accumulator Accumulator for tail recursion
   * @return Compound annual growth rate
   */
  @tailrec
  def calculateCompoundGrowthRate(salaryData: List[Double], periods: Int, accumulator: Double = 1.0): Double = {
    // validating arguments
    require(salaryData != null, "Salary data cannot be null")
    require(periods > 0, "Periods must be positive")

    salaryData match {
      case Nil => math.pow(accumulator, 1.0 / periods) - 1.0
      case head :: Nil => math.pow(accumulator, 1.0 / periods) - 1.0
      case head :: next :: tail =>
        val growthFactor = if (head > 0) next / head else 1.0
        calculateCompoundGrowthRate(next :: tail, periods, accumulator * growthFactor)
    }
  }

  /**
   * Pure function demonstrating function composition with combinators
   * Uses custom combinators from the trait for complex filtering
   *
   * @param companiesDataset Dataset of companies
   * @param minRating Minimum rating filter threshold
   * @param targetSector Target sector for filtering
   * @return Filtered dataset using composed filters
   */
  def advancedFilteringWithCombinators(companiesDataset: Dataset[Company],
                                       minRating: Double,
                                       targetSector: String): Dataset[Company] = {
    // validating arguments
    require(companiesDataset != null, "Companies dataset cannot be null")
    require(minRating >= 0 && minRating <= 5, "Rating must be between 0 and 5")
    require(targetSector != null && targetSector.trim.nonEmpty, "Target sector cannot be null or empty")

    // creating individual filter functions using currying
    val ratingFilter = Company.filterByMinRating(minRating)
    val sectorFilter = Company.filterBySector(targetSector)
    val highPotentialFilter: Company => Boolean = company => calculatePopularityScore(company) >= 50

    // combining filters using custom combinator
    val combinedFilterFunction = combineFilters(ratingFilter, sectorFilter, highPotentialFilter)

    // applying combined filter to dataset
    companiesDataset.filter(combinedFilterFunction)
  }

  /**
   * Pure function for comprehensive analytics using multiple Spark operations
   * Demonstrates composition of multiple transformations and aggregations
   *
   * @param companiesDataset Dataset of companies to analyze
   * @return Map containing various analytics results
   */
  def performComprehensiveAnalytics(companiesDataset: Dataset[Company]): Map[String, Any] = {
    // validating arguments
    require(companiesDataset != null, "Companies dataset cannot be null")

    import companiesDataset.sparkSession.implicits._

    // performing comprehensive analysis using multiple operations
    Map(
      "totalCompaniesCount" -> companiesDataset.count(),
      "averageCompanyRating" -> companiesDataset.filter(_.companyRatings > 0).agg(typed.avg(_.companyRatings)).collect()(0),
      "sectorDistributionData" -> companiesBySector(companiesDataset).collect().toMap,
      "topRatedCompanies" -> topCompaniesByRating(companiesDataset, 5).collect().toList,
      "ratingCategoriesDistribution" -> companiesDataset.map(categorizeByRating).groupByKey(identity).count().collect().toMap,
      "averageSalariesBySector" -> averageSalaryBySector(companiesDataset).collect().toMap
    )
  }

  /**
   * Pure function to save analysis results to file using functional approach
   * Separates I/O operations from pure computational logic
   *
   * @param datasetToSave Dataset to save to file
   * @param outputFilePath Output file path
   * @param spark Implicit SparkSession
   * @return Try indicating success or failure of save operation
   */
  def saveAnalysisResults[T](datasetToSave: Dataset[T], outputFilePath: String)
                            (implicit spark: SparkSession): Try[Unit] = {
    // validating arguments
    require(datasetToSave != null, "Dataset to save cannot be null")
    require(outputFilePath != null && outputFilePath.trim.nonEmpty, "Output path cannot be null or empty")
    require(spark != null, "SparkSession cannot be null")

    Try {
      // saving dataset as CSV with proper configuration
      datasetToSave
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(outputFilePath)
    }
  }

  /**
   * Pure function to calculate industry benchmarks
   * Uses statistical aggregations for benchmark analysis
   *
   * @param companiesDataset Dataset of companies
   * @return Map containing industry benchmark metrics
   */
  def calculateIndustryBenchmarks(companiesDataset: Dataset[Company]): Map[String, Double] = {
    // validating arguments
    require(companiesDataset != null, "Companies dataset cannot be null")

    import companiesDataset.sparkSession.implicits._

    // calculating various industry benchmarks
    val validCompaniesDataset = companiesDataset.filter(company =>
      company.companyRatings > 0 && company.avgSalary > 0)

    Map(
      "medianRating" -> calculateMedianRating(validCompaniesDataset),
      "averageReviewsCount" -> validCompaniesDataset.agg(typed.avg(_.totalReviews)).collect()(0),
      "averageSalaryOverall" -> validCompaniesDataset.agg(typed.avg(_.avgSalary)).collect()(0),
      "averageBenefitsOffered" -> validCompaniesDataset.agg(typed.avg(_.totalBenefits)).collect()(0)
    )
  }

  /**
   * Helper function to calculate median rating
   * Uses statistical computation for median calculation
   *
   * @param companiesDataset Dataset of companies with valid ratings
   * @return Median rating value
   */
  private def calculateMedianRating(companiesDataset: Dataset[Company]): Double = {
    // validating arguments
    require(companiesDataset != null, "Companies dataset cannot be null")

    import companiesDataset.sparkSession.implicits._

    // calculating median using statistical functions
    val ratingsArray = companiesDataset.map(_.companyRatings).sort($"value").collect()
    val arrayLength = ratingsArray.length

    if (arrayLength % 2 == 0) {
      (ratingsArray(arrayLength / 2 - 1) + ratingsArray(arrayLength / 2)) / 2.0
    } else {
      ratingsArray(arrayLength / 2)
    }
  }
}