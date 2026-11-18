"""
Version mappings and URL templates for arpe.io documentation sites.
"""

FASTBCP_DOC_VERSIONS = ["0.28", "0.29", "0.30", "0.31", "0.32", "1.0", "1.1", "1.2", "1.3", "1.4", "1.5", "1.6", "1.7"]
FASTTRANSFER_DOC_VERSIONS = ["0.14", "0.15", "0.16", "0.17", "0.18"]
# The LakeXpress site publishes 0.3 (at /0.3/) and 0.6 (at /latest/) only; a
# binary of another series falls back to the latest one.
LAKEXPRESS_DOC_VERSIONS = ["0.3", "0.6"]
# MigratorXpress docs are versioned by minor release (0.6.x -> "0.6", 0.7.x ->
# "0.7", 0.8.x -> "0.8"). The site has no page set per series for 0.7 and 0.8:
# the pages at /latest/ describe both (--with_xid and the license file of 0.8
# next to the 0.7 options), so both labels are served from there.
MIGRATORXPRESS_DOC_VERSIONS = ["0.6", "0.7", "0.8"]

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
    # One root sitemap.xml for the whole site (no per-version /<v>/sitemap
    # page); DOC_URL_SCOPES restricts the crawl to the selected version. It
    # lists the /latest/ pages only: the 0.6 pages are found by following links.
    "migratorxpress": "https://migratorxpress-docs.arpe.io/sitemap.xml",
    # Arpe ADBC drivers: one site for the whole family, documenting the current
    # release of each driver (no versioned URLs), cached as "adbc_latest".
    "adbc": "https://adbc-drivers-docs.arpe.io/sitemap.xml",
}

# Doc versions served under a different URL path. The current FastBCP and
# FastTransfer releases are published at /latest/ only (/1.7/ and /0.18/ return
# 404), and so are MigratorXpress 0.7 and 0.8 (/0.7/ and /0.8/ return 404) and
# LakeXpress 0.6; move these aliases forward when a newer version becomes
# "latest".
DOC_URL_VERSION_ALIASES = {
    "fastbcp": {"1.7": "latest"},
    "fasttransfer": {"0.18": "latest"},
    "lakexpress": {"0.6": "latest"},
    "migratorxpress": {"0.7": "latest", "0.8": "latest"},
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
