package io.github.jpicklyk.mcptask.current.infrastructure.sqlite

/**
 * Forgeyn Postgres persistence lane: the database dialect of a `DATABASE_PATH` value.
 *
 * The upstream engine is SQLite-only; Forgeyn deploys the orchestrator against Supabase Postgres
 * (durable persistence that survives container replacement), selected purely by the JDBC URL
 * prefix — no second environment variable, no ambiguous auto-detection.
 *
 * Every persistence decision that is dialect-specific branches on this value, and only on it:
 * pool construction, Flyway migration location, SQLite-only startup steps (compaction, integrity,
 * pre-upgrade file backup) and the FTS analyzer implementation.
 */
enum class DbDialect {
    SQLITE,
    POSTGRES;

    companion object {
        /** The one accepted Postgres JDBC URL prefix (pgjdbc's canonical form). */
        private const val POSTGRES_URL_PREFIX = "jdbc:postgresql:"

        /**
         * Resolves the dialect of a `DATABASE_PATH` value (a JDBC URL or, for SQLite, a file path).
         * Anything that is not a `jdbc:postgresql:` URL is SQLite: the historical default and the
         * upstream behavior are unchanged.
         */
        fun of(databasePath: String): DbDialect =
            if (databasePath.startsWith(POSTGRES_URL_PREFIX, ignoreCase = true)) POSTGRES else SQLITE
    }
}
