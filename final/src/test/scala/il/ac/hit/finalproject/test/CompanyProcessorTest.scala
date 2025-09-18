package il.ac.hit.finalproject.test

import il.ac.hit.finalproject.model.Company
import il.ac.hit.finalproject.processor.CompanyProcessor
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import scala.util.Success

/**
 * Unit tests for CompanyProcessor demonstrating functional testing principles.
 * Tests pure functions separately from I/O operations.
 * Follows functional programming testing best practices.
 */
class CompanyProcessorTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  // implicit SparkSession for testing operations
  implicit var spark: SparkSession = _

  /**
   * Setup method executed before all tests
   * Initializes SparkSession for testing environment
   */
  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .appName("CompanyProcessorTestSuite")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse-test")
      .getOrCreate()

    // reducing log noise during testing
    spark.sparkContext.setLogLevel("ERROR")
  }

  /**
   * Cleanup method executed after all tests
   * Properly shuts down SparkSession
   */
  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  // test data - immutable test fixtures following functional principles
  val testCompaniesData = List(
    Company("TCS", "IT Services & Consulting | 1 Lakh+ Employees", 3.8, "Job Security", "Promotions", 73100.0, 856.9, 6.1, 847, 11.5),
    Company("Accenture", "IT Services & Consulting | 1 Lakh+ Employees", 4.0, "Company Culture", "Salary", 46400.0, 584.6, 4.3, 9900, 7.1),
    Company("JPMorgan Chase", "Banking | 1 Lakh+ Employees", 4.2, "Job Security", "Work Pressure", 25000.0, 750.0, 3.5, 500, 8.0),
    Company("Johnson & Johnson", "Healthcare | 1 Lakh+ Employees", 4.1, "Benefits", "Bureaucracy", 18000.0, 680.0, 2.8, 300, 9.2),
    Company("Walmart", "Consumer Retail | 1 Lakh+ Employees", 3.5, "Job Security", "Low Pay", 35000.0, 420.0, 4.2, 1200, 5.8)
  )

  /**
   * Test case for Company.fromCsvRow factory method
   * Validates CSV parsing functionality with proper data transformation
   */
  "Company.fromCsvRow" should "parse CSV row data correctly with all fields" in {
    // preparing test CSV data
    val testCsvRow = Array("Test Company", "IT Services Description", "4.5", "Innovation", "Management", "50000.0", "800.0", "5.5", "1000", "12.0")

    // executing factory method
    val company = Company.fromCsvRow(testCsvRow)

    // validating all company properties
    company.companyName should be("Test Company")
    company.companyDescription should be("IT Services Description")
    company.companyRatings should be(4.5)
    company.highlyRatedFor should be("Innovation")
    company.criticallyRatedFor should be("Management")
    company.totalReviews should be(50000.0)
    company.avgSalary should be(800.0)
    company.interviewsTaken should be(5.5)
    company.totalJobsAvailable should be(1000)
    company.totalBenefits should be(12.0)
  }

  /**
   * Test case for Company.fromCsvRow with malformed numeric data
   * Validates error handling for invalid numeric values
   */
  "Company.fromCsvRow" should "handle malformed numeric data gracefully" in {
    // preparing CSV data with invalid numeric values
    val malformedCsvRow = Array("Test Company", "Description", "invalid", "Positive", "Negative", "abc", "xyz", "def", "non-numeric", "bad-number")

    // executing factory method with malformed data
    val company = Company.fromCsvRow(malformedCsvRow)

    // validating default values for malformed numeric fields
    company.companyName should be("Test Company")
    company.companyDescription should be("Description")
    company.companyRatings should be(0.0)
    company.totalReviews should be(0.0)
    company.avgSalary should be(0.0)
    company.interviewsTaken should be(0.0)
    company.totalJobsAvailable should be(0)
    company.totalBenefits should be(0.0)
  }

  /**
   * Test case for isHighlyRated property method
   * Validates rating classification logic
   */
  "Company.isHighlyRated" should "return true for companies with ratings >= 4.0" in {
    val highlyRatedCompany = testCompaniesData.find(_.companyRatings >= 4.0).get
    val lowRatedCompany = testCompaniesData.find(_.companyRatings < 4.0).get

    highlyRatedCompany.isHighlyRated should be(true)
    lowRatedCompany.isHighlyRated should be(false)
  }

  /**
   * Test case for extractSector method
   * Validates sector extraction logic from company descriptions
   */
  "Company.extractSector" should "correctly extract sector from company description" in {
    val itServicesCompany = testCompaniesData.find(_.companyDescription.toLowerCase.contains("it services")).get
    val bankingCompany = testCompaniesData.find(_.companyDescription.toLowerCase.contains("banking")).get

    itServicesCompany.extractSector should be("IT Services")
    bankingCompany.extractSector should be("Banking")
  }

  /**
   * Test case for popularityCategory method
   * Validates popularity classification based on review counts
   */
  "Company.popularityCategory" should "categorize companies by review popularity correctly" in {
    val veryPopularCompany = testCompaniesData.find(_.totalReviews >= 50000).get
    val moderatelyPopularCompany = testCompaniesData.find(company =>
      company.totalReviews >= 5000 && company.totalReviews < 20000).get

    veryPopularCompany.popularityCategory should be("Very Popular")
    moderatelyPopularCompany.popularityCategory should be("Moderately Popular")
  }

  /**
   * Test case for CompanyProcessor.companiesBySector method
   * Validates sector counting functionality using Spark operations
   */
  "CompanyProcessor.companiesBySector" should "count companies by sector categories correctly" in {
    import spark.implicits._

    // creating test dataset from test data
    val testDataset = testCompaniesData.toDS()

    // executing sector analysis
    val sectorCounts = CompanyProcessor.companiesBySector(testDataset)
    val results = sectorCounts.collect().toMap

    // validating sector counts
    results should contain key "Technology"
    results should contain key "Banking"
    results should contain key "Healthcare & Life Sciences"
    results.values.sum should be(testCompaniesData.length)
  }

  /**
   * Test case for CompanyProcessor.topCompaniesByRating method
   * Validates top companies selection and ordering functionality
   */
  "CompanyProcessor.topCompaniesByRating" should "return companies ordered by rating descending" in {
    import spark.implicits._

    // creating test dataset
    val testDataset = testCompaniesData.toDS()

    // executing top companies analysis
    val topCompanies = CompanyProcessor.topCompaniesByRating(testDataset, 3)
    val results = topCompanies.collect()

    // validating results ordering and count
    results.length should be <= 3
    if (results.length > 1) {
      results(0)._2 should be >= results(1)._2
    }
  }

  /**
   * Test case for CompanyProcessor.averageSalaryBySector method
   * Validates salary aggregation by sector functionality
   */
  "CompanyProcessor.averageSalaryBySector" should "calculate average salaries by sector correctly" in {
    import spark.implicits._

    // creating test dataset
    val testDataset = testCompaniesData.toDS()

    // executing salary analysis
    val sectorAverages = CompanyProcessor.averageSalaryBySector(testDataset)
    val results = sectorAverages.collect()

    // validating results structure and values
    results.length should be > 0
    results.foreach { case (sector, avgSalary) =>
      sector should not be empty
      avgSalary should be > 0.0
    }
  }

  /**
   * Test case for CompanyProcessor.findHighPotentialCompanies method
   * Validates complex filtering logic with multiple criteria
   */
  "CompanyProcessor.findHighPotentialCompanies" should "identify companies meeting high-potential criteria" in {
    import spark.implicits._

    // creating test dataset with high-potential companies
    val testDataset = testCompaniesData.toDS()

    // executing high-potential analysis
    val highPotentialCompanies = CompanyProcessor.findHighPotentialCompanies(testDataset)
    val results = highPotentialCompanies.collect()

    // validating that all results meet high-potential criteria
    results.foreach { company =>
      company.companyRatings should be >= 4.0
      CompanyProcessor.calculatePopularityScore(company) should be >= 60
    }
  }

  /**
   * Test case for curried filter functions
   * Validates functional programming currying implementation
   */
  "Company curried filter functions" should "work correctly with functional composition" in {
    // creating filter functions using currying
    val ratingFilter = Company.filterByMinRating(4.0)
    val sectorFilter = Company.filterBySector("Technology")
    val salaryFilter = Company.filterByMinSalary(500.0)

    // testing individual filters
    val highRatedCompanies = testCompaniesData.filter(ratingFilter)
    val technologyCompanies = testCompaniesData.filter(sectorFilter)
    val highSalaryCompanies = testCompaniesData.filter(salaryFilter)

    // validating filter results
    highRatedCompanies.foreach(_.companyRatings should be >= 4.0)
    technologyCompanies.foreach(_.extractSector should be("IT Services"))
    highSalaryCompanies.foreach(_.avgSalary should be >= 500.0)
  }

  /**
   * Test case for CompanyProcessor.calculateCompoundGrowthRate tail-recursive function
   * Validates tail recursion implementation for mathematical calculations
   */
  "CompanyProcessor.calculateCompoundGrowthRate" should "calculate growth rate using tail recursion correctly" in {
    // preparing test salary progression data
    val salaryProgression = List(100.0, 110.0, 121.0, 133.1)
    val periods = salaryProgression.length - 1

    // executing compound growth rate calculation
    val growthRate = CompanyProcessor.calculateCompoundGrowthRate(salaryProgression, periods)

    // validating calculated growth rate (approximately 10% per period)
    growthRate should be(0.10 +- 0.01)
  }

  /**
   * Test case for CompanyProcessor.performComprehensiveAnalytics method
   * Validates comprehensive analytics workflow integration
   */
  "CompanyProcessor.performComprehensiveAnalytics" should "return comprehensive analytics results map" in {
    import spark.implicits._

    // creating test dataset
    val testDataset = testCompaniesData.toDS()

    // executing comprehensive analytics
    val analyticsResults = CompanyProcessor.performComprehensiveAnalytics(testDataset)

    // validating analytics results structure
    analyticsResults should contain key "totalCompaniesCount"
    analyticsResults should contain key "averageCompanyRating"
    analyticsResults should contain key "sectorDistributionData"
    analyticsResults should contain key "topRatedCompanies"
    analyticsResults should contain key "ratingCategoriesDistribution"

    // validating data types and values
    analyticsResults("totalCompaniesCount") should be(testCompaniesData.length.toLong)
    analyticsResults("averageCompanyRating").asInstanceOf[Double] should be > 0.0
  }

  /**
   * Test case for custom combinators functionality
   * Validates combinator pattern implementation
   */
  "CompanyProcessor combinators" should "combine multiple filters correctly" in {
    // creating individual filter predicates
    val ratingFilter: Company => Boolean = _.companyRatings >= 4.0
    val popularityFilter: Company => Boolean = company => CompanyProcessor.calculatePopularityScore(company) >= 50
    val sectorFilter: Company => Boolean = _.extractSector == "IT Services"

    // combining filters using combinator
    val combinedFilter = CompanyProcessor.combineFilters(ratingFilter, popularityFilter, sectorFilter)

    // testing combined filter on test data
    val filteredCompanies = testCompaniesData.filter(combinedFilter)

    // validating that all results satisfy all filter conditions
    filteredCompanies.foreach { company =>
      company.companyRatings should be >= 4.0
      CompanyProcessor.calculatePopularityScore(company) should be >= 50
      company.extractSector should be("IT Services")
    }
  }
}