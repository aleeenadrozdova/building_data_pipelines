package ru.consultantplus.de

import org.apache.spark.sql.{Dataset, SparkSession}
import ru.consultantplus.de.metrics.Metrics
import ru.consultantplus.de.model.Event
import ru.consultantplus.de.parser.SessionParser

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

/**
 * Spark-задача для «КонсультантПлюс»:
 *   1. прочитать все файлы сессий (один файл = одна сессия);
 *   2. разобрать события (устойчиво к битым строкам и менявшемуся формату);
 *   3. посчитать две требуемые метрики;
 *   4. записать результаты в выходной каталог.
 *
 * Запуск (см. README.md):
 *   spark-submit --class ru.consultantplus.de.Main \
 *                --master local[*] target/de-task-1.0.0.jar \
 *                --input /путь/к/данным --out /путь/к/результату
 */
object Main {

  private final case class Args(input: String, out: String)

  // Числовые пары для метрики 2. id поисков уникальны в пределах сессии (файла),
  // поэтому привязку «документ найден через быстрый поиск» ведём по сессии.
  private final case class QuickHit(session: String, searchId: String, doc: String)
  private final case class DocOpenRow(session: String, day: String, searchId: String, doc: String)

  def main(args: Array[String]): Unit = {
    val opts = parseArgs(args)
    val spark = SparkSession.builder()
      .appName("ConsultantPlus-DE")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "8")
      .getOrCreate()
    try {
      run(spark, opts.input, opts.out)
    } finally {
      spark.stop()
    }
  }

  private def parseArgs(args: Array[String]): Args = {
    def valueOf(name: String): String = {
      val i = args.indexOf(name)
      if (i >= 0 && i + 1 < args.length) args(i + 1)
      else throw new IllegalArgumentException(s"Требуется аргумент $name --input/--out")
    }
    Args(valueOf("--input"), valueOf("--out"))
  }

  def run(spark: SparkSession, input: String, out: String): Unit = {
    import spark.implicits._

    // 1) файл = сессия; разбираем содержимое, считая битые строки
    val sessions = spark.sparkContext
      .wholeTextFiles(input)
      .map { case (path, content) => (path, SessionParser.parse(content, path)) }
      .cache()

    val sessionCount = sessions.count()
    val warnTotal = sessions.map(_._2.warnings.size).sum()
    println(s"[data] сессий (файлов): $sessionCount, событий: ${sessions.map(_._2.events.size).sum()}, предупреждений: $warnTotal")
    sessions
      .flatMap { case (p, r) => r.warnings.take(3).map(w => s"$p :: $w") }
      .take(10)
      .foreach(w => println(s"   ! $w"))

    // 2) события как Dataset
    val events: Dataset[Event] = sessions.flatMap(_._2.events).toDS()

    // ---------- Метрика 1 ----------
    // Сколько раз в карточке поиска искали документ ACC_45616
    // (карточный поиск, у которого параметр-«номер документа» = ACC_45616).
    val metric1 = events
      .filter(_.kind == Event.CardSearch)
      .filter(_.params.get(Metrics.Targets.DocKeyParam).contains(Metrics.Targets.DocumentId))
      .count()
    println(s"\nМетрика 1: карточных поисков документа ACC_45616 = $metric1\n")

    // ---------- Метрика 2 ----------
    // Открытие (DOC_OPEN) относится к конкретному поиску (searchId). Считаем
    // открытие, только если документ был в результатах именно того быстрого
    // поиска, на который ссылается открытие: привязка по (session, searchId, doc).
    val docOpens = sessions
      .flatMap { case (path, r) =>
        r.events.collect {
          case e
              if e.kind == Event.DocOpen &&
                e.day.nonEmpty && // открытия без даты (поздний формат) в разбивке «по дням» не входят
                e.searchId.nonEmpty && e.doc.nonEmpty =>
            DocOpenRow(path, e.day, e.searchId.get, e.doc.get)
        }
      }
      .toDS()

    val quickHits = sessions
      .flatMap { case (path, r) =>
        r.events.collect { case e if e.kind == Event.QuickSearch => e }
          .flatMap(e => e.docs.map(d => QuickHit(path, e.searchId.getOrElse(""), d)))
      }
      .toDS()
      .distinct()

    val metric2 = docOpens
      .join(quickHits, Seq("session", "searchId", "doc"), "inner")
      .groupBy("day", "doc")
      .count()
      .orderBy("day", "doc")

    val m2Rows = metric2.count()
    println(s"Метрика 2: уникальных пар (день, документ) с открытиями = $m2Rows")
    println("Пример первых строк (day, doc, count):")
    metric2.take(5).foreach(r => println(s"   ${r(0)}  ${r(1)}  ${r(2)}"))

    // 3) сохраняем результаты
    val outDir = Paths.get(out)
    Files.createDirectories(outDir)

    Files.write(
      outDir.resolve("metric1.txt"),
      (s"Кол-во карточных поисков документа ${Metrics.Targets.DocumentId}: $metric1\n").getBytes(StandardCharsets.UTF_8)
    )
    metric2.coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(outDir.resolve("metric2").toString)

    println(s"\n[out] метрика 1 -> ${outDir.resolve("metric1.txt")}")
    println(s"[out] метрика 2 -> ${outDir.resolve("metric2")}/ (CSV: day,doc,count)")
  }
}
