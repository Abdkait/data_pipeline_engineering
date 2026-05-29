package ru.hse.consultant

import org.scalatest.funsuite.AnyFunSuite

class LogParserSpec extends AnyFunSuite {

  private val Target = "ACC_45616"

  test("extractDay converts dd.MM.yyyy_HH:mm:ss to ISO day") {
    assert(LogParser.extractDay("01.07.2020_13:40:50").contains("2020-07-01"))
    assert(LogParser.extractDay("13.04.2020_22:16:17").contains("2020-04-13"))
    assert(LogParser.extractDay("garbage").isEmpty)
  }

  test("quick search openings are attributed to the correct day and document") {
    val session =
      """SESSION_START 01.07.2020_13:40:50
        |QS 01.07.2020_13:42:01 {test query}
        |155175031 PBI_253397 CJI_118221 LAW_316194
        |DOC_OPEN 01.07.2020_13:43:21 155175031 CJI_118221
        |DOC_OPEN 01.07.2020_13:46:01 155175031 LAW_316194
        |DOC_OPEN 01.07.2020_13:47:16 155175031 CJI_118221
        |SESSION_END 01.07.2020_13:53:46""".stripMargin

    val m = LogParser.parseSession(session, Target)

    assert(m.cardSearchesForTargetDoc == 0)
    val grouped = m.quickSearchDocOpens.groupBy(identity).map { case (k, v) => k -> v.size }
    assert(grouped(("2020-07-01", "CJI_118221")) == 2)
    assert(grouped(("2020-07-01", "LAW_316194")) == 1)
  }

  test("card search for the target document is counted once per block") {
    val session =
      """SESSION_START 01.05.2020_20:41:28
        |CARD_SEARCH_START 01.05.2020_20:43:27
        |$0 ACC_45616
        |CARD_SEARCH_END
        |23878480 ACC_45616 RLAW123_238416
        |DOC_OPEN 01.05.2020_20:43:50 23878480 ACC_45616
        |SESSION_END 01.05.2020_20:57:54""".stripMargin

    val m = LogParser.parseSession(session, Target)

    // The target appears as a card-search parameter -> Metric 1 counts the block once.
    assert(m.cardSearchesForTargetDoc == 1)
    // The DOC_OPEN belongs to a CARD search (not QS), so it must not appear in Metric 2.
    assert(m.quickSearchDocOpens.isEmpty)
  }

  test("DOC_OPEN without a timestamp falls back to the session day") {
    val session =
      """SESSION_START 19.02.2020_04:27:37
        |QS 19.02.2020_04:28:36 {q}
        |1482083015 CJI_97490 PBI_227324
        |DOC_OPEN  1482083015 CJI_97490
        |SESSION_END 19.02.2020_04:43:18""".stripMargin

    val m = LogParser.parseSession(session, Target)

    assert(m.quickSearchDocOpens == Seq(("2020-02-19", "CJI_97490")))
  }

  test("DOC_OPEN referencing an unknown search id is ignored, broken lines are tolerated") {
    val session =
      """SESSION_START 01.01.2021_10:00:00
        |TOTALLY_BROKEN_LINE !!!
        |DOC_OPEN 01.01.2021_10:05:00 999999 LAW_1
        |SESSION_END 01.01.2021_10:10:00""".stripMargin

    val m = LogParser.parseSession(session, Target)

    // search id 999999 was never registered as a search -> not attributed to QS
    assert(m.quickSearchDocOpens.isEmpty)
    assert(m.cardSearchesForTargetDoc == 0)
  }
}
