package ru.consultantplus.de.parser

import org.scalatest.funsuite.AnyFunSuite
import ru.consultantplus.de.model.Event
import ru.consultantplus.de.metrics.Metrics

class SessionParserSpec extends AnyFunSuite {

  private def parse(lines: String*): SessionParser.Result =
    SessionParser.parse(lines.mkString("\n"))

  test("разбирает сессию с быстрым поиском и открытиями") {
    val r = parse(
      "SESSION_START 01.07.2020_13:40:50",
      "QS 01.07.2020_13:42:01 {какой то запрос}",
      "155175031 PBI_253397 ACC_45614 LAW_182373",
      "DOC_OPEN 01.07.2020_13:43:21 155175031 ACC_45614",
      "SESSION_END 01.07.2020_13:53:46"
    )
    assert(r.warnings.isEmpty, r.warnings)
    assert(r.events.length == 2)

    val qs = r.events.head
    assert(qs.kind == Event.QuickSearch)
    assert(qs.day == "2020-07-01")
    assert(qs.searchId.contains("155175031"))
    assert(qs.docs == Seq("PBI_253397", "ACC_45614", "LAW_182373"))

    val open = r.events(1)
    assert(open.kind == Event.DocOpen)
    assert(open.doc.contains("ACC_45614"))
    assert(open.searchId.contains("155175031"))
  }

  test("разбирает карточку поиска: параметры, конец, результат") {
    val r = parse(
      "SESSION_START 16.05.2020_03:42:27",
      "CARD_SEARCH_START 16.05.2020_03:43:16",
      "$134 какой-то текст",
      "$0 ACC_45616",
      "CARD_SEARCH_END",
      "-1806568671 PBI_226911 CMB_18943 ACC_45616",
      "SESSION_END 16.05.2020_04:00:13"
    )
    assert(r.warnings.isEmpty, r.warnings)
    val card = r.events.head
    assert(card.kind == Event.CardSearch)
    assert(card.params("0") == "ACC_45616")
    assert(card.params("134") == "какой-то текст")
    assert(card.searchId.contains("-1806568671"))
    assert(card.docs.contains("ACC_45616"))
  }

  test("битые/неожиданные строки уходят в предупреждения, сессия не падает") {
    val r = parse(
      "SESSION_START 01.01.2020_00:00:00",
      "QWERTY битая строка",
      "QS 01.01.2020_00:01:00 {}",
      "неожиданный хвост",
      "DOC_OPEN 01.01.2020_00:02:00 abc недокумент",
      "SESSION_END 01.01.2020_00:05:00"
    )
    assert(r.warnings.nonEmpty)
    // QS без результата -> событие с пустым списком документов
    assert(r.events.count(_.kind == Event.QuickSearch) == 1)
    // битый DOC_OPEN не стал событием
    assert(r.events.count(_.kind == Event.DocOpen) == 0)
  }

  test("строка-результат без привязки (дубль) помечается предупреждением") {
    val r = parse(
      "SESSION_START 01.01.2020_00:00:00",
      "QS 01.01.2020_00:01:00 {}",
      "100 PBI_253397",
      "100 PBI_253397", // дубль результата
      "SESSION_END 01.01.2020_00:05:00"
    )
    assert(r.warnings.nonEmpty)
    assert(r.events.count(_.kind == Event.QuickSearch) == 1)
  }

  test("метрика 1: считаются только карточные поиски с нужным документом в параметре") {
    val r = parse(
      "CARD_SEARCH_START 01.01.2020_00:00:01",
      "$0 ACC_45616",
      "1 CMB_18765",
      "CARD_SEARCH_START 01.01.2020_00:01:01",
      "$0 ACC_99999",
      "2 ACC_45616", // документ в результатах, но НЕ в параметре
      "CARD_SEARCH_START 01.01.2020_00:02:01",
      "$0 ACC_45616",
      "3 ACC_45616"
    )
    assert(Metrics.cardSearchesForDocument(r.events) == 2)
  }

  test("метрика 2: привязка открытия к сессии (одинаковый id поиска в разных сессиях)") {
    // id поисков уникальны в пределах сессии; пара (100, LAW_1) из сессии «B»
    // не должна приписаться к поиску сессии «A».
    def qs(s: String, sid: String, doc: String, day: String): Event =
      Event(Event.QuickSearch, day, Some(sid), Map.empty, Seq(doc), None, s)
    def op(s: String, sid: String, doc: String, day: String): Event =
      Event(Event.DocOpen, day, Some(sid), Map.empty, Vector.empty, Some(doc), s)

    val events = Seq(
      qs("A", "100", "LAW_1", "2020-01-01"),
      op("A", "100", "LAW_1", "2020-01-02"),  // открыт из своего поиска -> учитываем
      op("B", "100", "LAW_1", "2020-01-03")   // та же пара, но сессия B — не её поиск
    )
    val m = Metrics.opensByDayForQuickSearchedDocuments(events)
    assert(m == Map(("2020-01-02", "LAW_1") -> 1L))
  }

  test("метрика 2: открытия только документов, найденных через быстрый поиск, по дням") {
    val r = parse(
      "QS 01.01.2020_00:01:00 {}",
      "10 LAW_100 LAW_200",
      "DOC_OPEN 01.01.2020_00:02:00 10 LAW_100", // открыт, найден в QS
      "DOC_OPEN 02.01.2020_00:02:00 10 LAW_100", // другой день
      "DOC_OPEN 01.01.2020_00:03:00 10 LAW_777", // НЕ найден в QS
      "QS 02.01.2020_00:01:00 {}",
      "11 LAW_777",
      "DOC_OPEN 02.01.2020_00:02:00 11 LAW_777"  // найден через QS 02.01
    )
    val m = Metrics.opensByDayForQuickSearchedDocuments(r.events)
    assert(m == Map(
      ("2020-01-01", "LAW_100") -> 1L,
      ("2020-01-02", "LAW_100") -> 1L,
      ("2020-01-02", "LAW_777") -> 1L
    ))
  }

  test("DOC_OPEN позднего формата (без даты) парсится, но в метрике 2 не виден") {
    val r = parse(
      "QS 01.01.2020_00:01:00 {}",
      "10 LAW_900",
      // поздний формат: нет даты, DOC_OPEN <search_id> <док>
      "DOC_OPEN 10 LAW_900"
    )
    assert(r.warnings.isEmpty, r.warnings)
    val open = r.events.find(_.kind == Event.DocOpen).get
    assert(open.doc.contains("LAW_900"))
    assert(open.day.isEmpty)
    assert(Metrics.opensByDayForQuickSearchedDocuments(r.events).isEmpty)
  }

  test("идентификаторы документов с цифрами в базе распознаются (RLAW080_115230)") {
    val r = parse(
      "QS 01.01.2020_00:01:00 {}",
      "10 RLAW080_115230 REXP140_38180",
      "DOC_OPEN 01.01.2020_00:02:00 10 RLAW080_115230"
    )
    assert(r.warnings.isEmpty, r.warnings)
    val qs = r.events.find(_.kind == Event.QuickSearch).get
    assert(qs.docs == Seq("RLAW080_115230", "REXP140_38180"))
    val m = Metrics.opensByDayForQuickSearchedDocuments(r.events)
    assert(m == Map(("2020-01-01", "RLAW080_115230") -> 1L))
  }
}
