package ru.consultantplus.de.metrics

import ru.consultantplus.de.model.Event

/**
 * Определения метрик, требуемых заданием. Чистые функции (без Spark) —
 * используются как эталон в тестах; реальный подсчёт в [[ru.consultantplus.de.Main]]
 * идёт теми же правилами через Spark DataFrames.
 */
object Metrics {

  object Targets {
    /** Документ, который ищем в «карточке» (метрика 1). */
    val DocumentId = "ACC_45616"
    /** Идентификатор параметра карточки «номер документа» (по данным $0). */
    val DocKeyParam = "0"
  }

  /**
   * Метрика 1: сколько раз в карточке поиска искали документ `docId`
   * (карточный поиск с параметром-«номером документа», равным `docId`).
   */
  def cardSearchesForDocument(events: Seq[Event], docId: String = Targets.DocumentId): Long =
    events.count(e => e.kind == Event.CardSearch &&
      e.params.get(Targets.DocKeyParam).contains(docId))

  /**
   * Метрика 2: для документа, найденного через быстрый поиск, число открытий
   * `DOC_OPEN` по дням. Возвращает (day, doc) -> count.
   *
   * Открытие относится к конкретному поиску (поле `searchId`), поэтому
   * «найден через быстрый поиск» проверяется привязкой по тройке
   * (session, searchId, doc): документ считается найденным, если он появился
   * в результатах именно того быстрого поиска, на который ссылается `DOC_OPEN`.
   * Открытия без даты (поздний формат) в разбивке «по дням» не участвуют.
   */
  def opensByDayForQuickSearchedDocuments(events: Seq[Event]): Map[(String, String), Long] = {
    val quickHits: Set[(String, String, String)] =
      events.collect { case e if e.kind == Event.QuickSearch =>
        e.docs.map(d => (e.session, e.searchId.getOrElse(""), d))
      }.flatten.toSet
    events
      .collect {
        case e
            if e.kind == Event.DocOpen &&
              e.day.nonEmpty &&
              e.doc.nonEmpty &&
              quickHits.contains((e.session, e.searchId.getOrElse(""), e.doc.get)) =>
          (e.day, e.doc.get)
      }
      .groupBy(identity)
      .view
      .mapValues(_.size.toLong)
      .toMap
  }
}
