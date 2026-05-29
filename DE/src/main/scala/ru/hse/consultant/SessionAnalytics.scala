package ru.hse.consultant

import org.apache.spark.sql.SparkSession

/**
 * Spark job that computes two usage metrics over ConsultantPlus session logs:
 *
 *   1. How many card searches (CARD_SEARCH blocks) were performed for the document `ACC_45616`
 *      (i.e. used it as a search parameter value).
 *   2. For every document found via a quick search (QS), the number of openings per day.
 *
 * Each input file is a single session. Because parsing relies on the order of lines inside a
 * session (a result line follows its search; a DOC_OPEN references a search by id), sessions are
 * read whole via `wholeTextFiles` and parsed independently. This avoids cross-session id
 * collisions and keeps the logic in a single, well-controlled RDD pass.
 *
 * Usage:
 *   SessionAnalytics <inputPath> <outputPath>
 * Defaults: inputPath = "Data", outputPath = "output".
 */
object SessionAnalytics {

  /** Document id whose card-search usage is counted in Metric 1. */
  val TargetDoc: String = "ACC_45616"

  def main(args: Array[String]): Unit = {
    val inputPath = args.lift(0).getOrElse("Data")
    val outputPath = args.lift(1).getOrElse("output")

    val spark = SparkSession
      .builder()
      .appName("ConsultantPlus Session Analytics")
      .master(sys.props.getOrElse("spark.master", "local[*]"))
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    // Observability: counters that are reported but never drive business logic
    // (accumulators may overcount on retries, so they are used for diagnostics only).
    val sessionCount = sc.longAccumulator("sessions")
    val badLineCount = sc.longAccumulator("badLines")

    try {
      // One record per session file -> one SessionMetrics. Cached because it feeds two actions.
      val parsed = sc
        .wholeTextFiles(inputPath)
        .map { case (_, content) =>
          sessionCount.add(1)
          val metrics = LogParser.parseSession(content, TargetDoc)
          badLineCount.add(metrics.badLines)
          metrics
        }
        .cache()

      // Metric 1: total number of card searches that targeted TargetDoc.
      // `fold` (not `reduce`) is safe on an empty RDD; no data is pulled to the driver.
      val metric1 = parsed.map(_.cardSearchesForTargetDoc).fold(0L)(_ + _)

      // Metric 2: openings per (day, document) for documents found via quick search.
      // reduceByKey performs map-side combine -> no groupByKey, no driver-side collection.
      val metric2 = parsed
        .flatMap(_.quickSearchDocOpens.map(pair => (pair, 1L)))
        .reduceByKey(_ + _)
        .map { case ((day, docId), opens) => (day, docId, opens) }

      // Persist Metric 2 as a single CSV (small result set), sorted for readability.
      import spark.implicits._
      val metric2Df = metric2
        .toDF("day", "document_id", "open_count")
        .orderBy($"day".asc, $"open_count".desc, $"document_id".asc)

      metric2Df
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(s"$outputPath/quick_search_doc_opens_by_day")

      // Persist Metric 1 as a tiny single-file text result.
      sc.parallelize(Seq(s"card_searches_for_$TargetDoc\t$metric1"), numSlices = 1)
        .saveAsTextFile(s"$outputPath/card_searches_for_target")

      // Console summary (uses take, never collect on the full dataset).
      val separator = "=" * 64
      println(separator)
      println(s"Sessions processed:                 ${sessionCount.value}")
      println(s"Bad/skipped lines (diagnostic):     ${badLineCount.value}")
      println(s"[Metric 1] Card searches for $TargetDoc: $metric1")
      println(s"[Metric 2] Top 10 (day, document, open_count):")
      metric2
        .sortBy({ case (_, _, opens) => opens }, ascending = false)
        .take(10)
        .foreach { case (day, docId, opens) => println(f"    $day%-12s $docId%-16s $opens") }
      println(s"Results written under: $outputPath/")
      println(separator)
    } finally {
      spark.stop()
    }
  }
}
