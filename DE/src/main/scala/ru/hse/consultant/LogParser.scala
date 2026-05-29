package ru.hse.consultant

import scala.collection.mutable

/**
 * Result of parsing a single user session (one input file).
 *
 * @param cardSearchesForTargetDoc number of CARD_SEARCH blocks in which the target document id
 *                                 was used as a search parameter value (Metric 1 contribution)
 * @param quickSearchDocOpens      for every DOC_OPEN that belongs to a quick search (QS),
 *                                 a pair (day, documentId) (Metric 2 contribution)
 * @param badLines                 number of lines that could not be parsed and were skipped
 */
final case class SessionMetrics(
    cardSearchesForTargetDoc: Long,
    quickSearchDocOpens: Seq[(String, String)],
    badLines: Long
)

/**
 * Pure (Spark-independent) parser for ConsultantPlus session logs.
 *
 * Each session is a sequence of lines. The grammar (with tolerated variations) is:
 *
 *   SESSION_START dd.MM.yyyy_HH:mm:ss
 *   QS dd.MM.yyyy_HH:mm:ss {query text}
 *   <searchId> <docId> <docId> ...                 // result line that follows QS
 *   CARD_SEARCH_START dd.MM.yyyy_HH:mm:ss
 *   $<paramId> <param value ...>                   // one or more parameter lines
 *   CARD_SEARCH_END
 *   <searchId> <docId> <docId> ...                 // result line that follows the card search
 *   DOC_OPEN dd.MM.yyyy_HH:mm:ss <searchId> <docId> // normal form (4 fields)
 *   DOC_OPEN <searchId> <docId>                     // legacy form without timestamp (3 fields)
 *   SESSION_END dd.MM.yyyy_HH:mm:ss
 *
 * The parser is defensive: malformed lines are counted and skipped instead of failing the job.
 */
object LogParser {

  /** Marker for the search type associated with a given searchId within a session. */
  private object SearchKind {
    val Quick: Char = 'Q'
    val Card: Char = 'C'
  }

  private val TimestampRegex = """\d{1,2}\.\d{1,2}\.\d{4}_.*""".r.pattern
  private val SearchIdRegex = """-?\d+""".r.pattern

  private def isTimestamp(token: String): Boolean =
    token.contains("_") && TimestampRegex.matcher(token).matches()

  private def isSearchId(token: String): Boolean =
    SearchIdRegex.matcher(token).matches()

  /** Converts a `dd.MM.yyyy_HH:mm:ss` timestamp into an ISO `yyyy-MM-dd` day, if possible. */
  def extractDay(token: String): Option[String] = {
    val datePart = token.split("_", 2)(0)
    val parts = datePart.split("\\.")
    if (parts.length != 3) None
    else {
      try {
        val dd = "%02d".format(parts(0).toInt)
        val mm = "%02d".format(parts(1).toInt)
        val yyyy = parts(2)
        if (yyyy.length == 4) Some(s"$yyyy-$mm-$dd") else None
      } catch {
        case _: NumberFormatException => None
      }
    }
  }

  private def splitTokens(line: String): Array[String] = line.trim.split("\\s+")

  private def nextNonEmptyIndex(lines: Array[String], from: Int): Int = {
    var j = from
    while (j < lines.length && lines(j).trim.isEmpty) j += 1
    if (j < lines.length) j else -1
  }

  /**
   * Parses one session.
   *
   * @param content   full text content of a single session file
   * @param targetDoc document id whose card-search usage is counted for Metric 1
   */
  def parseSession(content: String, targetDoc: String): SessionMetrics = {
    val lines = content.split("\n")

    // searchId -> kind (Quick / Card); built incrementally so DOC_OPEN can be classified.
    val searchKind = mutable.HashMap.empty[String, Char]
    val quickOpens = mutable.ListBuffer.empty[(String, String)]
    var metric1 = 0L
    var badLines = 0L

    // Last timestamp seen in this session, used as a fallback day for DOC_OPEN without a timestamp.
    var lastDay: Option[String] = None

    /** Registers the result line that follows a search and records its searchId -> kind. */
    def consumeResultLine(startIdx: Int, kind: Char): Int = {
      val j = nextNonEmptyIndex(lines, startIdx)
      if (j < 0) startIdx
      else {
        val tokens = splitTokens(lines(j))
        if (tokens.nonEmpty && isSearchId(tokens(0))) {
          searchKind(tokens(0)) = kind
          j + 1
        } else {
          // Result line is missing/broken: do not consume it, let the main loop handle it.
          startIdx
        }
      }
    }

    var i = 0
    while (i < lines.length) {
      val line = lines(i).trim
      if (line.isEmpty) {
        i += 1
      } else {
        val tokens = splitTokens(line)
        val head = tokens(0)
        try {
          head match {
            case "SESSION_START" | "SESSION_END" =>
              if (tokens.length >= 2) extractDay(tokens(1)).foreach(d => lastDay = Some(d))
              i += 1

            case "QS" =>
              if (tokens.length >= 2) extractDay(tokens(1)).foreach(d => lastDay = Some(d))
              i = consumeResultLine(i + 1, SearchKind.Quick)

            case "CARD_SEARCH_START" =>
              if (tokens.length >= 2) extractDay(tokens(1)).foreach(d => lastDay = Some(d))
              // Scan parameter lines until CARD_SEARCH_END.
              var k = i + 1
              var ended = false
              var hitTarget = false
              while (k < lines.length && !ended) {
                val paramLine = lines(k).trim
                if (paramLine.startsWith("CARD_SEARCH_END")) {
                  ended = true
                } else {
                  if (paramLine.startsWith("$")) {
                    val paramTokens = paramLine.split("\\s+")
                    // tokens after "$id" are the parameter value(s)
                    if (paramTokens.drop(1).contains(targetDoc)) hitTarget = true
                  }
                  k += 1
                }
              }
              if (hitTarget) metric1 += 1
              i = if (ended) consumeResultLine(k + 1, SearchKind.Card) else k

            case "DOC_OPEN" =>
              // Two tolerated shapes:
              //   DOC_OPEN <ts> <searchId> <docId>  (timestamp present)
              //   DOC_OPEN <searchId> <docId>       (timestamp missing)
              val (day, searchId, docId) =
                if (tokens.length >= 4 && isTimestamp(tokens(1))) {
                  val d = extractDay(tokens(1))
                  d.foreach(x => lastDay = Some(x))
                  (d.orElse(lastDay), tokens(2), tokens(3))
                } else if (tokens.length >= 3) {
                  (lastDay, tokens(1), tokens(2))
                } else {
                  throw new IllegalArgumentException("DOC_OPEN with too few fields")
                }

              if (searchKind.get(searchId).contains(SearchKind.Quick)) {
                quickOpens += ((day.getOrElse("UNKNOWN"), docId))
              }
              i += 1

            case _ =>
              // Orphan result line or unknown token: skip without failing.
              i += 1
          }
        } catch {
          case _: Throwable =>
            badLines += 1
            i += 1
        }
      }
    }

    SessionMetrics(metric1, quickOpens.toList, badLines)
  }
}
