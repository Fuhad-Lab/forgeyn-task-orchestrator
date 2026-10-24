package io.github.jpicklyk.mcptask.current.infrastructure.sqlite.upgrade

import org.jetbrains.exposed.v1.core.vendors.PostgreSQLDialect
import org.jetbrains.exposed.v1.core.vendors.currentDialect

/**
 * Forgeyn Postgres persistence lane: the dialect-aware SQL fragments of the 3.x-to-4.0 backfill
 * data steps. Each fragment has an identical meaning on both dialects:
 *
 *  - [uuidKey]: the correlation key of a uuid column — SQLite stores ids as BLOBs and derives a
 *    text key with `lower(hex(col))`; Postgres's native uuid renders through `lower(col::text)`
 *    (hyphenated). The key only ever joins two queries of the same step, so any deterministic
 *    per-value form is correct as long as both queries use the same one.
 *  - the row selector: SQLite's implicit `rowid` addresses a `work_items` row; Postgres has no
 *    rowid, and the `id` uuid column is the row address.
 *
 * Call [postgres] once, inside the step's write unit, and branch every fragment of that step on
 * the single value so the two queries of a step can never disagree.
 */
internal object BackfillSql {
    /** True when the current transaction's database is Postgres (the Supabase persistence lane). */
    fun postgres(): Boolean = currentDialect is PostgreSQLDialect

    /** The text correlation key of a uuid column, for either dialect. */
    fun uuidKey(col: String,
        isPostgres: Boolean
    ): String = if (isPostgres) "lower($col::text)" else "lower(hex($col))"

    /** The `work_items` row-address column: `id` on Postgres, the implicit `rowid` on SQLite. */
    fun rowColumn(isPostgres: Boolean): String = if (isPostgres) "id" else "rowid"
}
