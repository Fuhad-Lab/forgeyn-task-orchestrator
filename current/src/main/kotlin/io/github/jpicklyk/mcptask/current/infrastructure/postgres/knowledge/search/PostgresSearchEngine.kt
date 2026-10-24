package io.github.jpicklyk.mcptask.current.infrastructure.postgres.knowledge.search

import io.github.jpicklyk.mcptask.current.application.knowledge.related.SimilarTerms
import io.github.jpicklyk.mcptask.current.application.port.Analyzer
import io.github.jpicklyk.mcptask.current.application.port.Candidate
import io.github.jpicklyk.mcptask.current.application.port.Corpus
import io.github.jpicklyk.mcptask.current.application.port.DocumentHitMeta
import io.github.jpicklyk.mcptask.current.application.port.MAX_TRAVERSAL_DEPTH
import io.github.jpicklyk.mcptask.current.application.port.ScopeFilter
import io.github.jpicklyk.mcptask.current.domain.model.EntityRef
import io.github.jpicklyk.mcptask.current.domain.model.RefKind
import io.github.jpicklyk.mcptask.current.application.port.SearchIndex
import io.github.jpicklyk.mcptask.current.application.port.TextOp
import io.github.jpicklyk.mcptask.current.application.port.TextQuery
import io.github.jpicklyk.mcptask.current.infrastructure.sqlite.DatabaseManager
import io.github.jpicklyk.mcptask.current.infrastructure.sqlite.repository.LikeLiteral
import io.github.jpicklyk.mcptask.current.infrastructure.sqlite.repository.readTx
import io.github.jpicklyk.mcptask.current.infrastructure.sqlite.repository.rawQuery
import io.github.jpicklyk.mcptask.current.infrastructure.sqlite.repository.uuidOf
import io.github.jpicklyk.mcptask.current.infrastructure.sqlite.schema.WorkItemsTable
import org.jetbrains.exposed.v1.core.ColumnType
import org.jetbrains.exposed.v1.core.IntegerColumnType
import org.jetbrains.exposed.v1.core.java.UUIDColumnType
import org.jetbrains.exposed.v1.core.VarCharColumnType
import org.jetbrains.exposed.v1.core.dao.id.EntityID
import org.jetbrains.exposed.v1.core.inList
import org.jetbrains.exposed.v1.jdbc.select
import java.util.UUID

/**
 * Forgeyn Postgres persistence lane: the [SearchIndex] over native Postgres search, replacing the
 * SQLite FTS5 engine when DATABASE_PATH is a `jdbc:postgresql:` URL. Selected in
 * [io.github.jpicklyk.mcptask.current.infrastructure.sqlite.repository.DefaultRepositoryProvider].
 *
 * The two upstream analyzers map onto Postgres index structures created by the baseline migration
 * (db/migration/postgres/V1__Postgres_Baseline.sql):
 *  - [Analyzer.STEMMED] (FTS5 porter + unicode61)  -> the generated `fts_text` tsvector column with
 *    a GIN index, queried with `websearch_to_tsquery` (caller text is quoted into phrases, so no
 *    term is ever parsed as syntax; stopwords drop out instead of erroring) and ranked with
 *    negated `ts_rank` (the port's rank is lower-is-better, like FTS5's bm25 ordering).
 *  - [Analyzer.SUBSTRING] (FTS5 trigram)           -> `ILIKE '%term%'` predicates served by the
 *    pg_trgm GIN indexes on the indexed text columns. A term shorter than 3 characters never
 *    matches, exactly as the port documents.
 *
 * Snippets carry `<mark>`/`</mark>` around matched text with a `…` ellipsis, mirroring FTS5's
 * `snippet()` presentation: STEMMED via `ts_headline`, SUBSTRING via a Kotlin-side window around
 * the first case-insensitive occurrence.
 *
 * Scope predicates reproduce the SQLite engine's forms verbatim where they are portable
 * (root/role/tag/trait filters, the bounded ancestor walk) and replace the one SQLite-specific
 * recursive tag split (the tags_include allowlist) with Postgres's `string_to_table` + `btrim` —
 * the same exact-equality-after-trim rule.
 *
 * [highlights] keeps its default (empty) implementation: [candidates] rows already carry their
 * field and snippet, so no second read is ever needed.
 */
class PostgresSearchEngine(
    private val databaseManager: DatabaseManager
) : SearchIndex {

    override suspend fun candidates(
        corpus: Corpus,
        query: TextQuery,
        analyzer: Analyzer,
        filter: ScopeFilter,
        window: Int
    ): List<Candidate> {
        if (window < 1) return emptyList()
        if (filter.rootIds != null && filter.rootIds.isEmpty()) return emptyList()
        require(corpus.itemFacets || !filter.hasItemFacets()) {
            "role, tags and traits do not apply to the ${corpus.kind} corpus"
        }
        val descriptor = PostgresCorpus.of(corpus)

        val match: MatchSql? =
            when (analyzer) {
                Analyzer.STEMMED -> stemmedMatch(descriptor, query)
                Analyzer.SUBSTRING -> substringMatch(descriptor, query)
            }
        // A match that can never hold (ALL-op with a sub-3-char term, ANY-op with none of them).
        if (match == null) return emptyList()

        val scope = ScopeSql.build(descriptor.ownerColumn, filter)
        val statement = buildStatement(descriptor, match, analyzer, scope, window)
        return databaseManager.readTx {
            rawQuery(statement.sql, statement.args) { rs ->
                val out = mutableListOf<Candidate>()
                while (rs.next()) {
                    // Select order: 1=rank, 2=doc_id, 3=owner_id, 4=note_key (NULL for item and
                    // document corpora), then one <i>_match boolean + <i>_text text pair per field
                    // in precedence order, then the corpus's extra columns.
                    val rank = (rs.getObject(1) as Number).toDouble()
                    val id = uuidOf(rs.getObject(2)!!)
                    val owner = uuidOf(rs.getObject(3)!!)
                    val noteKey = if (descriptor.noteKeyColumn != null) rs.getString(4) ?: "" else null
                    val fieldsBase = 5
                    val (field, snippet) =
                        descriptor.fields.withIndex()
                            .firstOrNull { (rs.getObject(fieldsBase + it.index * 2) as? Boolean) == true }
                            ?.let { (index, f) -> f.field to (rs.getString(fieldsBase + index * 2 + 1) ?: "") }
                            ?: (descriptor.fields.first().field to "")
                    val extra = fieldsBase + descriptor.fields.size * 2
                    val document =
                        if (corpus == Corpus.DOCUMENT) {
                            DocumentHitMeta(
                                kind = rs.getString(extra) ?: "",
                                slug = rs.getString(extra + 1) ?: "",
                                title = rs.getString(extra + 2),
                                version = (rs.getObject(extra + 3) as Number).toInt(),
                            )
                        } else {
                            null
                        }
                    val rendered =
                        if (analyzer == Analyzer.SUBSTRING) {
                            renderSubstringSnippet(match.substringTerms, snippet)
                        } else {
                            snippet
                        }
                    out +=
                        Candidate(
                            id = id,
                            ownerItemId = owner,
                            noteKey = noteKey,
                            rank = rank,
                            field = field,
                            snippet = rendered,
                            document = document,
                        )
                }
                out
            }
        }
    }

    override suspend fun titles(itemIds: Set<UUID>): Map<UUID, String> {
        if (itemIds.isEmpty()) return emptyMap()
        return databaseManager.readTx {
            val entityIds = itemIds.map { EntityID(it, WorkItemsTable) }
            WorkItemsTable
                .select(WorkItemsTable.id, WorkItemsTable.title)
                .where { WorkItemsTable.id inList entityIds }
                .associate { it[WorkItemsTable.id].value to it[WorkItemsTable.title] }
        }
    }

    override suspend fun similarTerms(
        ref: EntityRef,
        max: Int
    ): List<String> {
        if (max < 1) return emptyList()
        val texts =
            databaseManager.readTx {
                val out = mutableListOf<String>()
                val args = listOf<Pair<ColumnType<*>, Any?>>(uuidType to ref.id)

                fun read(sql: String, columns: Int) = rawQuery(sql, args) { rs ->
                    while (rs.next()) {
                        for (c in 1..columns) rs.getString(c)?.let { out += it }
                    }
                }
                when (ref.kind) {
                    RefKind.ITEM -> {
                        read("SELECT title, summary, description FROM work_items WHERE id = ?", 3)
                        read("SELECT body FROM notes WHERE work_item_id = ?", 1)
                    }
                    RefKind.NOTE -> read("SELECT body FROM notes WHERE id = ?", 1)
                    RefKind.DOCUMENT -> read("SELECT title, body FROM documents WHERE id = ?", 2)
                }
                out
            }
        return SimilarTerms.extract(texts, max)
    }

    override suspend fun taggedItems(
        tags: List<String>,
        filter: ScopeFilter
    ): Map<UUID, String?> {
        val wanted = tags.map { it.trim().lowercase() }.filter { it.isNotEmpty() }.distinct()
        if (wanted.isEmpty()) return emptyMap()
        if (filter.rootIds != null && filter.rootIds.isEmpty()) return emptyMap()
        val scope = ScopeSql.build("wi.id", filter.copy(tagsAny = wanted))
        val where = mutableListOf<String>()
        where += scope.predicates
        // The tag membership filter: case-insensitive membership of the comma-separated list,
        // each tag a literal (LIKE metacharacters escaped), identical to the request-tags filter.
        val esc = LikeLiteral.ESCAPE_CLAUSE
        where +=
            wanted.joinToString(" OR ", prefix = "(", postfix = ")") {
                "(LOWER(wi.tags) = ? OR LOWER(wi.tags) LIKE ? $esc OR LOWER(wi.tags) LIKE ? $esc" +
                    " OR LOWER(wi.tags) LIKE ? $esc)"
            }
        val args = scope.args.toMutableList()
        for (t in wanted) {
            val e = LikeLiteral.escape(t)
            args += textType to t
            args += textType to "$e,%"
            args += textType to "%,$e,%"
            args += textType to "%,$e"
        }
        val sql =
            """
            ${scope.ctes}
            SELECT wi.id, wi.tags
            FROM work_items wi
            WHERE ${where.joinToString("\n AND ")}
            """.trimIndent()
        return databaseManager.readTx {
            rawQuery(sql, args) { rs ->
                val out = LinkedHashMap<UUID, String?>()
                while (rs.next()) out[uuidOf(rs.getObject(1)!!)] = rs.getString(2)
                out
            }
        }
    }

    // ------------------------------------------------------------------
    // Match rendering
    // ------------------------------------------------------------------

    /**
     * STEMMED match: one websearch query text serves the row filter, the rank, the per-field match
     * booleans and the per-field headlines. Terms are quoted into websearch phrases so no term is
     * ever parsed as an operator; ALL joins phrases with a space (websearch AND), ANY with the
     * literal ` OR ` between the quoted phrases.
     */
    private fun stemmedMatch(
        descriptor: PostgresCorpus,
        query: TextQuery
    ): MatchSql {
        val phrases = query.terms.map { '"' + it.replace("\"", " ") + '"' }
        val queryText =
            when (query.op) {
                TextOp.ALL -> phrases.joinToString(" ")
                TextOp.ANY -> phrases.joinToString(" OR ")
            }
        return MatchSql(
            predicate = "${descriptor.ftsColumn} @@ websearch_to_tsquery('english', ?)",
            args = listOf(textType to queryText),
            rankExpression = "-ts_rank(${descriptor.ftsColumn}, websearch_to_tsquery('english', ?))",
            rankArgs = listOf(textType to queryText),
            queryText = queryText,
        )
    }

    /**
     * SUBSTRING match: per term, an OR over the corpus's fields; the per-term groups combine with
     * AND for [TextOp.ALL] and OR for [TextOp.ANY]. A term shorter than 3 characters never
     * matches, so an ALL query containing one matches nothing, and an ANY query keeps only its
     * eligible terms.
     */
    private fun substringMatch(
        descriptor: PostgresCorpus,
        query: TextQuery
    ): MatchSql? {
        val eligible = query.terms.filter { it.length >= 3 }
        if (eligible.isEmpty()) return null
        if (query.op == TextOp.ALL && eligible.size < query.terms.size) return null
        val joiner = if (query.op == TextOp.ALL) " AND " else " OR "
        val esc = LikeLiteral.ESCAPE_CLAUSE
        val predicate =
            eligible.joinToString(joiner, prefix = "(", postfix = ")") { term ->
                val pattern = "%${LikeLiteral.escape(term)}%"
                descriptor.fields.joinToString(" OR ", prefix = "(", postfix = ")") { f ->
                    "(${f.column} IS NOT NULL AND ${f.column} ILIKE ? $esc)"
                }
            }
        val args =
            eligible.flatMap { term ->
                val pattern = "%${LikeLiteral.escape(term)}%"
                List(descriptor.fields.size) { textType to pattern }
            }
        // Constant rank: substring ordering was never meaningfully scored upstream either (FTS5
        // trigram bm25 over raw substrings); the doc_id tiebreak keeps the order deterministic.
        return MatchSql(
            predicate = predicate,
            args = args,
            rankExpression = "1.0",
            rankArgs = emptyList(),
            substringTerms = eligible,
        )
    }

    // ------------------------------------------------------------------
    // Statement assembly
    // ------------------------------------------------------------------

    /** One rendered statement and its positional arguments, in placeholder order. */
    private data class Statement(
        val sql: String,
        val args: List<Pair<ColumnType<*>, Any?>>,
    )

    /** The match half of a [candidates] statement. */
    private data class MatchSql(
        val predicate: String,
        val args: List<Pair<ColumnType<*>, Any?>>,
        val rankExpression: String,
        val rankArgs: List<Pair<ColumnType<*>, Any?>>,
        /** The STEMMED websearch query text, reused for per-field match and headline columns. */
        val queryText: String? = null,
        /** The SUBSTRING terms, reused for per-field match columns and snippet rendering. */
        val substringTerms: List<String> = emptyList(),
    )

    /** The scope half: the optional WITH RECURSIVE clause plus predicates and their args. */
    private class ScopeSql(
        val ctes: String,
        val predicates: List<String>,
        val args: List<Pair<ColumnType<*>, Any?>>,
    ) {
        companion object {
            fun build(ownerColumn: String, filter: ScopeFilter): ScopeSql {
            val ctes = mutableListOf<String>()
            val cteArgs = mutableListOf<Pair<ColumnType<*>, Any?>>()
            val predicates = mutableListOf<String>()
            val args = mutableListOf<Pair<ColumnType<*>, Any?>>()

            filter.itemId?.let {
                predicates += "$ownerColumn = ?"
                args += uuidType to it
            }
            filter.ancestorId?.let {
                ctes +=
                    """
                    anc_subtree(id, lvl) AS (
                        SELECT id, 1 FROM work_items WHERE id = ?
                        UNION ALL
                        SELECT c.id, s.lvl + 1 FROM work_items c JOIN anc_subtree s ON c.parent_id = s.id
                        WHERE s.lvl < $MAX_TRAVERSAL_DEPTH
                    )
                    """.trimIndent()
                cteArgs += uuidType to it
                predicates += "$ownerColumn IN (SELECT id FROM anc_subtree)"
            }
            filter.rootIds?.let { roots ->
                val placeholders = roots.joinToString(", ") { "?" }
                // Roots only: the owning item's denormalized root_id must be a listed id. No walk;
                // an id that is not a project root admits nothing.
                predicates += "wi.root_id IN ($placeholders)"
                roots.forEach { args += uuidType to it }
            }
            filter.roles?.takeIf { it.isNotEmpty() }?.let { roles ->
                predicates += "wi.role IN (${roles.joinToString(", ") { "?" }})"
                roles.forEach { args += textType to it.name.lowercase() }
            }
            val requestTags =
                filter.tagsAny
                    ?.map { it.trim().lowercase() }
                    ?.filter { it.isNotEmpty() }
                    .orEmpty()
            if (requestTags.isNotEmpty()) {
                val esc = LikeLiteral.ESCAPE_CLAUSE
                predicates +=
                    requestTags.joinToString(" OR ", prefix = "(", postfix = ")") {
                        "(LOWER(wi.tags) = ? OR LOWER(wi.tags) LIKE ? $esc OR LOWER(wi.tags) LIKE ? $esc" +
                            " OR LOWER(wi.tags) LIKE ? $esc)"
                    }
                for (t in requestTags) {
                    val e = LikeLiteral.escape(t)
                    args += textType to t
                    args += textType to "$e,%"
                    args += textType to "%,$e,%"
                    args += textType to "%,$e"
                }
            }
            val requestTraits =
                filter.traitsAny
                    ?.map { it.trim() }
                    ?.filter { it.isNotEmpty() }
                    ?.distinct()
                    .orEmpty()
            if (requestTraits.isNotEmpty()) {
                predicates +=
                    "EXISTS (SELECT 1 FROM item_traits it WHERE it.item_id = $ownerColumn" +
                        " AND it.name IN (${requestTraits.joinToString(", ") { "?" }}))"
                requestTraits.forEach { args += textType to it }
            }
            filter.principalTagsAny?.takeIf { it.isNotEmpty() }?.let { allowed ->
                // The tags_include access rule in its Postgres form: split the comma-separated
                // tags, trim each element, require exact equality with a listed tag. The same
                // rule the SQLite engine enforced through its recursive tag_split CTE.
                predicates +=
                    "EXISTS (SELECT 1 FROM string_to_table(wi.tags, ',') AS split(tag) " +
                        "WHERE btrim(split.tag) IN (${allowed.joinToString(", ") { "?" }}))"
                allowed.forEach { args += textType to it }
            }
            val with = if (ctes.isEmpty()) "" else "WITH RECURSIVE " + ctes.joinToString(",\n") + "\n"
                return ScopeSql(with, predicates, cteArgs + args)
            }
        }
    }

    private fun buildStatement(
        descriptor: PostgresCorpus,
        match: MatchSql,
        analyzer: Analyzer,
        scope: ScopeSql,
        window: Int
    ): Statement {
        val where = mutableListOf<String>()
        where += match.predicate
        where += descriptor.fixedPredicates
        where += scope.predicates

        val noteKey = descriptor.noteKeyColumn ?: "NULL"
        val fieldColumns =
            descriptor.fields.withIndex().joinToString("") { (i, f) ->
                when (analyzer) {
                    Analyzer.STEMMED -> {
                        val tsq = "websearch_to_tsquery('english', ?)"
                        ",\n(to_tsvector('english', coalesce(${f.column}, '')) @@ $tsq) AS f${i}_match" +
                            ",\nts_headline('english', coalesce(${f.column}, ''), $tsq, '$HEADLINE_OPTS') AS f${i}_text"
                    }
                    Analyzer.SUBSTRING -> {
                        val esc = LikeLiteral.ESCAPE_CLAUSE
                        ",\n(${match.substringTerms.joinToString(" OR ") {
                            "(${f.column} IS NOT NULL AND ${f.column} ILIKE ? $esc)"
                        }}) AS f${i}_match" +
                            ",\n${f.column} AS f${i}_text"
                    }
                }
            }
        val extraSelect = descriptor.extraColumns.joinToString("") { ", $it" }
        val sql =
            """
            ${scope.ctes}
            SELECT ${match.rankExpression} AS rank, ${descriptor.idColumn} AS doc_id, ${descriptor.ownerColumn} AS owner_id, $noteKey AS note_key$fieldColumns$extraSelect
            FROM ${descriptor.fromClause}
            WHERE ${where.joinToString("\n AND ")}
            ORDER BY rank, doc_id
            LIMIT ?
            """.trimIndent()

        val args = mutableListOf<Pair<ColumnType<*>, Any?>>()
        args += match.rankArgs
        args += match.args
        when (analyzer) {
            Analyzer.STEMMED -> {
                // Per field: the match boolean's query param, then the headline's.
                val queryText = match.queryText!!
                repeat(descriptor.fields.size) {
                    args += textType to queryText
                    args += textType to queryText
                }
            }
            Analyzer.SUBSTRING -> {
                // Per field: one param per substring term, in the predicate's join order.
                repeat(descriptor.fields.size) {
                    match.substringTerms.forEach { term ->
                        args += textType to "%${LikeLiteral.escape(term)}%"
                    }
                }
            }
        }
        args += scope.args
        args += intType to window
        return Statement(sql, args)
    }

    /**
     * Renders the SUBSTRING snippet in Kotlin from the raw field text: a window of at most
     * [SNIPPET_WINDOW] characters around the first case-insensitive occurrence of any term, the
     * occurrence wrapped in `<mark>`/`</mark>`, `…` marking both truncations — the FTS5
     * `snippet()` presentation.
     */
    private fun renderSubstringSnippet(
        terms: List<String>,
        text: String
    ): String {
        if (text.isEmpty()) return ""
        val lower = text.lowercase()
        var at = -1
        var hit = ""
        for (term in terms) {
            val idx = lower.indexOf(term.lowercase())
            if (idx >= 0 && (at < 0 || idx < at)) {
                at = idx
                hit = term
            }
        }
        if (at < 0) return text.take(SNIPPET_WINDOW)
        val start = (at - SNIPPET_WINDOW / 2).coerceAtLeast(0)
        val end = (at + hit.length + SNIPPET_WINDOW / 2).coerceAtMost(text.length)
        val prefix = if (start > 0) "…" else ""
        val suffix = if (end < text.length) "…" else ""
        val body = text.substring(start, at) + "<mark>" + text.substring(at, at + hit.length) + "</mark>" +
            text.substring(at + hit.length, end)
        return prefix + body + suffix
    }

    private fun ScopeFilter.hasItemFacets(): Boolean =
        !roles.isNullOrEmpty() || !tagsAny.isNullOrEmpty() || !traitsAny.isNullOrEmpty()

    private companion object {
        private val uuidType = UUIDColumnType()
        private val textType = VarCharColumnType(4000)
        private val intType = IntegerColumnType()

        /** The SUBSTRING snippet window, in characters. */
        private const val SNIPPET_WINDOW = 160

        /**
         * ts_headline options mirroring the FTS5 snippet() presentation: `<mark>` delimiters, the
         * `…` ellipsis, at most 2 fragments of at most 32 words each.
         */
        private const val HEADLINE_OPTS =
            "StartSel=<mark>, StopSel=</mark>, MaxFragments=2, MaxWords=32, MinWords=8, FragmentDelimiter= … "
    }
}

/** One indexed field of a corpus: the field name a hit reports and the column it reads. */
private data class PostgresField(
    val field: String,
    val column: String,
)

/** Everything the Postgres engine needs to know about one corpus; the engine is generic, each corpus is data. */
private data class PostgresCorpus(
    val corpus: Corpus,
    val fromClause: String,
    val idColumn: String,
    val ownerColumn: String,
    val noteKeyColumn: String?,
    val fields: List<PostgresField>,
    val ftsColumn: String,
    val fixedPredicates: List<String> = emptyList(),
    val extraColumns: List<String> = emptyList(),
) {
    companion object {
        /** Work-item title, summary and description, owned by the item itself. */
        val ITEM =
            PostgresCorpus(
                corpus = Corpus.ITEM,
                fromClause = "work_items wi",
                idColumn = "wi.id",
                ownerColumn = "wi.id",
                noteKeyColumn = null,
                fields =
                    listOf(
                        PostgresField("title", "wi.title"),
                        PostgresField("summary", "wi.summary"),
                        PostgresField("description", "wi.description"),
                    ),
                ftsColumn = "wi.fts_text",
            )

        /** Note bodies, owned by the note's work item (joined as `wi` for the item-facet filters). */
        val NOTE =
            PostgresCorpus(
                corpus = Corpus.NOTE,
                fromClause = "notes n JOIN work_items wi ON wi.id = n.work_item_id",
                idColumn = "n.id",
                ownerColumn = "n.work_item_id",
                noteKeyColumn = "n.key",
                fields = listOf(PostgresField("body", "n.body")),
                ftsColumn = "n.fts_text",
            )

        /** Document title and body, owned by the document's root item; archived rows never match. */
        val DOCUMENT =
            PostgresCorpus(
                corpus = Corpus.DOCUMENT,
                fromClause = "documents d JOIN work_items wi ON wi.id = d.root_item_id",
                idColumn = "d.id",
                ownerColumn = "d.root_item_id",
                noteKeyColumn = null,
                fields =
                    listOf(
                        PostgresField("title", "d.title"),
                        PostgresField("body", "d.body"),
                    ),
                ftsColumn = "d.fts_text",
                fixedPredicates = listOf("d.archived_at IS NULL"),
                extraColumns = listOf("d.kind", "d.slug", "d.title", "d.version"),
            )

        fun of(corpus: Corpus): PostgresCorpus =
            when (corpus) {
                Corpus.ITEM -> ITEM
                Corpus.NOTE -> NOTE
                Corpus.DOCUMENT -> DOCUMENT
            }
    }
}
