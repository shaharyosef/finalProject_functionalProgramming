package il.ac.hit.finalproject.analytics

import il.ac.hit.finalproject.model.Company

/**
 * Trait defining pure analytical functions for company data processing.
 * Follows functional programming principles with immutable operations.
 * All methods are pure functions without side effects.
 */
trait CompanyAnalytics {

  /**
   * Pure function to categorize company sector into broader categories
   * Uses pattern matching for classification based on description analysis
   *
   * @param company The company to categorize
   * @return Broad sector category as string
   */
  def categorizeSector(company: Company): String = {
    // validating arguments
    require(company != null, "Company cannot be null")

    company.extractSector match {
      case "IT Services" => "Technology"
      case "Banking" => "Financial Services"
      case "Healthcare" => "Healthcare & Life Sciences"
      case "Telecommunications" => "Communications"
      case "Retail" => "Consumer Services"
      case _ => "Other Industries"
    }
  }

  /**
   * Pure function to categorize company by rating ranges
   * Uses pattern matching with rating thresholds
   *
   * @param company The company to categorize
   * @return Rating category as string
   */
  def categorizeByRating(company: Company): String = {
    // validating arguments
    require(company != null, "Company cannot be null")

    company.companyRatings match {
      case rating if rating >= 4.5 => "Excellent"
      case rating if rating >= 4.0 => "Very Good"
      case rating if rating >= 3.5 => "Good"
      case rating if rating >= 3.0 => "Average"
      case rating if rating >= 2.0 => "Below Average"
      case _ => "Poor"
    }
  }

  /**
   * Pure function to calculate popularity score based on reviews
   * Demonstrates functional computation without side effects
   *
   * @param company The company to analyze
   * @return Popularity score as integer
   */
  def calculatePopularityScore(company: Company): Int = {
    // validating arguments
    require(company != null, "Company cannot be null")

    val reviewsScore = company.totalReviews match {
      case reviews if reviews >= 50000 => 100
      case reviews if reviews >= 20000 => 80
      case reviews if reviews >= 10000 => 60
      case reviews if reviews >= 5000 => 40
      case reviews if reviews >= 1000 => 20
      case _ => 10
    }

    val ratingsBonus = if (company.companyRatings >= 4.0) 20 else 0

    // calculating final popularity score
    reviewsScore + ratingsBonus
  }

  /**
   * Pure function to determine if company offers competitive benefits
   * Higher-order function example with threshold comparison
   *
   * @param company The company to evaluate
   * @param industryAverage Average benefits in the industry
   * @return true if benefits are competitive
   */
  def hasCompetitiveBenefits(company: Company, industryAverage: Double): Boolean = {
    // validating arguments
    require(company != null, "Company cannot be null")
    require(industryAverage >= 0, "Industry average cannot be negative")

    company.totalBenefits >= (industryAverage * 0.9)
  }

  /**
   * Custom combinator for composing company filters
   * Demonstrates combinator pattern implementation
   *
   * @param filters Variable number of filter functions
   * @return Combined filter function that applies all filters
   */
  def combineFilters(filters: (Company => Boolean)*): Company => Boolean = {
    // validating arguments
    require(filters != null, "Filters cannot be null")
    require(filters.nonEmpty, "At least one filter must be provided")

    company => filters.forall(filter => filter(company))
  }

  /**
   * Custom combinator for chaining transformations
   * Another combinator pattern example for function composition
   *
   * @param transformations Variable number of transformation functions
   * @return Combined transformation function
   */
  def chainTransformations[A](transformations: (A => A)*): A => A = {
    // validating arguments
    require(transformations != null, "Transformations cannot be null")
    require(transformations.nonEmpty, "At least one transformation must be provided")

    transformations.reduce(_ andThen _)
  }

  /**
   * Pure function to calculate company attractiveness score
   * Combines multiple factors using functional composition
   *
   * @param company The company to evaluate
   * @return Attractiveness score as double
   */
  def calculateAttractivenessScore(company: Company): Double = {
    // validating arguments
    require(company != null, "Company cannot be null")

    // calculating individual component scores
    val ratingScore = company.companyRatings * 20.0
    val popularityScore = calculatePopularityScore(company) * 0.5
    val salaryScore = math.min(company.avgSalary / 10.0, 50.0)
    val benefitsScore = math.min(company.totalBenefits / 100.0, 30.0)

    // combining scores using weighted average
    (ratingScore * 0.4) + (popularityScore * 0.3) + (salaryScore * 0.2) + (benefitsScore * 0.1)
  }

  /**
   * Pure function to identify growth potential based on job availability
   * Uses mathematical calculation for growth assessment
   *
   * @param company The company to analyze
   * @return Growth potential category as string
   */
  def assessGrowthPotential(company: Company): String = {
    // validating arguments
    require(company != null, "Company cannot be null")

    val jobsToInterviewsRatio = if (company.interviewsTaken > 0) {
      company.totalJobsAvailable.toDouble / company.interviewsTaken
    } else {
      0.0
    }

    jobsToInterviewsRatio match {
      case ratio if ratio >= 2.0 => "High Growth"
      case ratio if ratio >= 1.0 => "Moderate Growth"
      case ratio if ratio >= 0.5 => "Steady Growth"
      case _ => "Limited Growth"
    }
  }
}