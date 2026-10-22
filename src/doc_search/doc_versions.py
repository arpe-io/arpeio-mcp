"""
Version mappings and URL templates for arpe.io documentation sites.
"""

FASTBCP_DOC_VERSIONS = ["0.28", "0.29", "0.30", "0.31", "0.32", "1.0", "1.1", "1.2", "1.3", "1.4"]
FASTTRANSFER_DOC_VERSIONS = ["0.14", "0.15", "0.16", "0.17", "0.18"]
LAKEXPRESS_DOC_VERSIONS = ["0.2", "0.3", "0.4"]
# MigratorXpress docs are versioned by minor release (0.6.x -> "0.6", 0.7.x -> "0.7").
MIGRATORXPRESS_DOC_VERSIONS = ["0.6", "0.7"]

# Release-notes URL templates ({version} is substituted with e.g. "0.31")
RELEASE_NOTES_URLS = {
    "fastbcp": "https://fastbcp-docs.arpe.io/latest/release-notes/release-notes-{version}",
    "fasttransfer": "https://fasttransfer-docs.arpe.io/latest/release-notes/release-notes-{version}",
    "lakexpress": "https://lakexpress-docs.arpe.io/latest/release-notes/release-notes-{version}",
}

# Products whose docs publish no release-notes pages: `arpe_release_notes` says
# so and points to this docs URL (MigratorXpress only has a "What's new in 0.7"
# summary on the 0.7 intro page, returned when indexed).
RELEASE_NOTES_UNPUBLISHED = {
    "migratorxpress": "https://migratorxpress-docs.arpe.io/latest/",
}

DOC_URLS = {
    "fastbcp": "https://fastbcp-docs.arpe.io/{version}/sitemap",
    "fasttransfer": "https://fasttransfer-docs.arpe.io/{version}/sitemap",
    "lakexpress": "https://lakexpress-docs.arpe.io/{version}/sitemap",
    # One root sitemap.xml lists every version (no per-version /<v>/sitemap
    # page); DOC_URL_SCOPES restricts the crawl to the selected version.
    "migratorxpress": "https://migratorxpress-docs.arpe.io/sitemap.xml",
    # Arpe ADBC drivers: one site for the whole family, documenting the current
    # release of each driver (no versioned URLs), cached as "adbc_latest".
    "adbc": "https://adbc-drivers-docs.arpe.io/sitemap.xml",
}

# Doc versions served under a different URL path. The current FastBCP and
# FastTransfer releases are published at /latest/ only (/1.4/ and /0.18/ return
# 404), and so is MigratorXpress 0.7 (/0.7/ returns 404); move these aliases
# forward when a newer version becomes "latest".
DOC_URL_VERSION_ALIASES = {
    "fastbcp": {"1.4": "latest"},
    "fasttransfer": {"0.18": "latest"},
    "migratorxpress": {"0.7": "latest"},
}

# Sites whose single sitemap covers several doc versions: only the sitemap URLs
# starting with this prefix ({version} after DOC_URL_VERSION_ALIASES) are crawled.
DOC_URL_SCOPES = {
    "migratorxpress": "https://migratorxpress-docs.arpe.io/{version}/",
}

BLOG_URL = "https://blog.arpe.io/"

# Map product names to their version lists for matching detected versions
VERSION_LISTS = {
    "fastbcp": FASTBCP_DOC_VERSIONS,
    "fasttransfer": FASTTRANSFER_DOC_VERSIONS,
    "lakexpress": LAKEXPRESS_DOC_VERSIONS,
    "migratorxpress": MIGRATORXPRESS_DOC_VERSIONS,
}

# TTL in days
DOC_TTL_DAYS = 7
BLOG_TTL_DAYS = 30
