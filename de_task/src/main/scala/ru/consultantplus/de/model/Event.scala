package ru.consultantplus.de.model

/**
 * Единое представление события пользовательской сессии «КонсультантПлюс».
 *
 * Константы [[Event.QuickSearch]], [[Event.CardSearch]], [[Event.DocOpen]] в [[Event.kind]]
 * позволяют хранить событие плоской записью, которую легко превратить в Spark Dataset.
 *
 * Поля заполняются по смыслу события:
 *   - QuickSearch: `day`, `searchId`, `docs` (найденные документы);
 *   - CardSearch:  `day`, `params` (id параметра -> значение), `searchId`, `docs`;
 *   - DocOpen:     `day`, `searchId` (какому поиску соответствует), `doc`.
 *
 * `session` — идентификатор сессии (файла). id поисков уникальны в пределах
 * сессии (в данных 10 id встречаются в двух сессиях), поэтому привязка
 * `DOC_OPEN` -> результат поиска идёт по (session, searchId, doc).
 */
final case class Event(
    kind: String,
    day: String,
    searchId: Option[String],
    params: Map[String, String],
    docs: Seq[String],
    doc: Option[String],
    session: String
)

object Event {
  val QuickSearch = "quick_search"
  val CardSearch = "card_search"
  val DocOpen = "doc_open"
}
