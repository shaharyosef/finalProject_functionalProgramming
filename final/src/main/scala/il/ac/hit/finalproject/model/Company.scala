package il.ac.hit.finalproject.model

/**
 * Immutable case class representing a company in the dataset.
 * Follows functional programming principles with immutable data structures.
 *
 * @param companyName Company name
 * @param companyDescription Company description and details
 * @param companyRatings Overall company rating
 * @param highlyRatedFor What the company is highly rated for
 * @param criticallyRatedFor What the company is critically rated for
 * @param totalReviews Total number of reviews
 * @param avgSalary Average salary in the company
 * @param interviewsTaken Number of interviews taken
 * @param totalJobsAvailable Total jobs available
 * @param totalBenefits Total benefits offered
 */
case class Company(
                    companyName: String,
                    companyDescription: String,
                    companyRatings: Double,
                    highlyRatedFor: String,
                    criticallyRatedFor: String,
                    totalReviews: Double,
                    avgSalary: Double,
                    interviewsTaken: Double,
                    totalJobsAvailable: Long,
                    totalBenefits: Double
                  ) {

  /**
   * Pure function to check if company has high ratings (greater than 4.0)
   *
   * @return true if highly rated, false otherwise
   */
  def isHighlyRated: Boolean = companyRatings >= 4.0

  /**
   * Pure function to get company popularity category based on total reviews
   * Uses pattern matching for classification
   *
   * @return popularity category as string
   */
  def popularityCategory: String = totalReviews match {
    case reviews if reviews >= 50000 => "Very Popular"
    case reviews if reviews >= 20000 => "Popular"
    case reviews if reviews >= 5000 => "Moderately Popular"
    case _ => "Less Popular"
  }

  /**
   * Pure function to extract sector from description
   * Uses pattern matching with string analysis
   *
   * @return sector category extracted from description
   */
  def extractSector: String = companyDescription.toLowerCase match {
    case desc if desc.contains("it services") || desc.contains("consulting") => "IT Services"
    case desc if desc.contains("banking") => "Banking"
    case desc if desc.contains("telecom") => "Telecommunications"
    case desc if desc.contains("pharma") || desc.contains("healthcare") => "Healthcare"
    case desc if desc.contains("retail") => "Retail"
    case _ => "Other"
  }

  /**
   * Pure function to calculate salary attractiveness score
   *
   * @return salary score based on average salary ranges
   */
  def salaryScore: String = avgSalary match {
    case salary if salary >= 1000 => "Excellent Salary"
    case salary if salary >= 500 => "Good Salary"
    case salary if salary >= 200 => "Average Salary"
    case _ => "Below Average Salary"
  }
}

/**
 * Companion object for Company case class
 * Contains factory methods and utility functions
 */
object Company {

  /**
   * Factory method to create Company from CSV row using pure functions
   * Uses functional error handling and validation
   *
   * @param csvRow Array of string values from CSV
   * @return Company instance or throws exception for invalid data
   */
  def fromCsvRow(csvRow: Array[String]): Company = {
    // validating arguments
    require(csvRow != null, "CSV row cannot be null")
    require(csvRow.length >= 10, "CSV row must contain at least 10 columns")

    // helper function to safely parse Double values
    def safeParseDouble(str: String): Double = {
      val cleanStr = str.trim.replaceAll("[^0-9.-]", "")
      if (cleanStr.isEmpty || cleanStr == "-") 0.0
      else try {
        cleanStr.toDouble
      } catch {
        case _: NumberFormatException => 0.0
      }
    }

    // helper function to safely parse Long values
    def safeParseLong(str: String): Long = {
      val cleanStr = str.trim.replaceAll("[^0-9]", "")
      if (cleanStr.isEmpty) 0L
      else try {
        cleanStr.toLong
      } catch {
        case _: NumberFormatException => 0L
      }
    }

    // creating company instance using parsed values
    Company(
      companyName = csvRow(0).trim,
      companyDescription = csvRow(1).trim,
      companyRatings = safeParseDouble(csvRow(2)),
      highlyRatedFor = csvRow(3).trim,
      criticallyRatedFor = csvRow(4).trim,
      totalReviews = safeParseDouble(csvRow(5)),
      avgSalary = safeParseDouble(csvRow(6)),
      interviewsTaken = safeParseDouble(csvRow(7)),
      totalJobsAvailable = safeParseLong(csvRow(8)),
      totalBenefits = safeParseDouble(csvRow(9))
    )
  }

  /**
   * Curried function for filtering companies by minimum rating
   * Demonstrates functional programming currying technique
   *
   * @param minRating minimum rating threshold
   * @return function that filters companies by rating
   */
  val filterByMinRating: Double => Company => Boolean =
    minRating => company => company.companyRatings >= minRating

  /**
   * Curried function for filtering companies by sector
   * Demonstrates functional programming currying technique
   *
   * @param sector target sector for filtering
   * @return function that filters companies by sector
   */
  val filterBySector: String => Company => Boolean =
    sector => company => company.extractSector.equalsIgnoreCase(sector)

  /**
   * Curried function for filtering companies by minimum salary
   * Demonstrates functional programming currying technique
   *
   * @param minSalary minimum salary threshold
   * @return function that filters companies by salary
   */
  val filterByMinSalary: Double => Company => Boolean =
    minSalary => company => company.avgSalary >= minSalary
}