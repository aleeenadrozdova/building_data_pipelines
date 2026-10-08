package ru.consultantplus.de.parser

import ru.consultantplus.de.model.Event

/**
 * Разбирает текстовый лог одной пользовательской сессии (один файл = одна сессия)
 * в последовательность событий [[Event]].
 *
 * Формат строк (изучили по предоставленным данным; формат мог меняться, записи
 * местами битые — поэтому парсер устойчив и считает пропуски):
 *   SESSION_START <dd.MM.yyyy_HH:mm:ss>
 *   QS <dd.MM.yyyy_HH:mm:ss> {<текст запроса>}
 *   <search_id> <док1> <док2> ...        — результат быстрого поиска
 *   CARD_SEARCH_START <dd.MM.yyyy_HH:mm:ss>
 *   $<id параметра> <значение>           — параметры карточки поиска
 *   CARD_SEARCH_END
 *   <search_id> <док1> <док2> ...        — результат карточного поиска
 *   DOC_OPEN <dd.MM.yyyy_HH:mm:ss> <search_id> <док>
 *
 * Текст (запросы, значения) приходит в cp1251; для метрик он не нужен, поэтому
 * работаем со строками как с последовательностью байт-безопасных токенов и не
 * зависим от кодировки (структурные префиксы — ASCII).
 */
object SessionParser {

  final case class Result(events: Vector[Event], warnings: Vector[String])

  private val DayRe = """(\d{2})\.(\d{2})\.(\d{4})_""".r
  // База документа может содержать цифры (RLAW080_115230, REXP140_38180): буквы+цифры до '_'
  private val DocRe = """[A-Za-z][A-Za-z0-9]{1,19}_\d+""".r
  private val ParamRe = """\$(\S+)\s*(.*)""".r
  private val ResultRe = """(-?\d+)(?:\s+(.*))?""".r

  private def isResultLine(line: String): Boolean =
    """-?\d+(\s|$)""".r.findPrefixOf(line).isDefined && line.nonEmpty

  private def dayOf(line: String): Option[String] =
    DayRe.findFirstIn(line).flatMap { s =>
      val p = s.split('.')
      Some(p(2).take(4) + "-" + p(1) + "-" + p(0))
    }

  private def resultIds(line: String): (String, Vector[String]) =
    line match {
      case ResultRe(id, rest) =>
        val docs = Option(rest).map(DocRe.findAllMatchIn(_).map(_.matched).toVector).getOrElse(Vector.empty)
        (id, docs)
      case _ => ("", Vector.empty)
    }

  def parse(content: String, session: String = ""): Result = {
    val lines = content.split("\\r?\\n")
    val events = Vector.newBuilder[Event]
    val warnings = Vector.newBuilder[String]
    var i = 0

    def warn(msg: String): Unit = warnings += s"line ${i + 1}: $msg"

    while (i < lines.length) {
      val line = lines(i).trim
      if (line.isEmpty) {
        i += 1
      } else if (line.startsWith("SESSION_START") || line.startsWith("SESSION_END")) {
        i += 1
      } else if (line.startsWith("QS ")) {
        val day = dayOf(line).getOrElse("")
        var searchId: Option[String] = None
        var docs: Vector[String] = Vector.empty
        i += 1
        // результат быстрого поиска — следующая непустая строка
        var attached = false
        while (i < lines.length && !attached) {
          val next = lines(i).trim
          if (next.isEmpty) i += 1
          else if (isResultLine(next)) {
            val (sid, ds) = resultIds(next)
            searchId = Some(sid); docs = ds; i += 1; attached = true
          } else {
            warn("у QS нет строки-результата, следующая строка не похожа на результат")
            attached = true
          }
        }
        events += Event(Event.QuickSearch, day, searchId, Map.empty, docs, None, session)
      } else if (line.startsWith("CARD_SEARCH_START")) {
        val day = dayOf(line).getOrElse("")
        val params = scala.collection.mutable.Map.empty[String, String]
        var searchId: Option[String] = None
        var docs: Vector[String] = Vector.empty
        i += 1
        var done = false
        while (i < lines.length && !done) {
          val next = lines(i).trim
          if (next.isEmpty) i += 1
          else if (next.startsWith("$")) {
            next match {
              case ParamRe(id, value) => params.update(id, value.trim)
              case _                  => warn(s"не распознан параметр карточки: $next")
            }
            i += 1
          } else if (next.startsWith("CARD_SEARCH_END")) {
            i += 1
          } else if (isResultLine(next)) {
            val (sid, ds) = resultIds(next)
            searchId = Some(sid); docs = ds; i += 1; done = true
          } else {
            warn(s"неожиданная строка внутри карточки поиска: ${next.take(80)}")
            done = true
          }
        }
        events += Event(Event.CardSearch, day, searchId, params.toMap, docs, None, session)
      } else if (line.startsWith("DOC_OPEN ")) {
        val tokens = line.split("\\s+")
        // основной формат: DOC_OPEN <дд.мм.гггг_чч:мм:сс> <search_id> <док>
        // поздний формат (конец датасета): DATE отсутствует: DOC_OPEN <search_id> <док>
        val sid = if (tokens.length >= 4) tokens(2) else if (tokens.length >= 3) tokens(1) else ""
        val doc = if (tokens.length >= 4) tokens(3) else if (tokens.length >= 3) tokens(2) else ""
        if (sid.matches("-?\\d+") && doc.matches("""[A-Za-z][A-Za-z0-9]{1,19}_\d+""")) {
          // у позднего формата нет даты -> день пустой, из «по дням» он исключается
          events += Event(Event.DocOpen, dayOf(line).getOrElse(""), Some(sid), Map.empty, Vector.empty, Some(doc), session)
        } else {
          warn(s"битая строка DOC_OPEN: ${line.take(120)}")
        }
        i += 1
      } else if (isResultLine(line)) {
        warn(s"строка-результат без привязки к поиску (дубль?): ${line.take(80)}")
        i += 1
      } else {
        warn(s"неизвестная строка: ${line.take(80)}")
        i += 1
      }
    }

    Result(events.result(), warnings.result())
  }
}
