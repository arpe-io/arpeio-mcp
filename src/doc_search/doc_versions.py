"""
Version mappings and URL templates for arpe.io documentation sites.
"""

FASTBCP_DOC_VERSIONS = ["0.28", "0.29", "0.30", "0.31", "0.32"]
FASTTRANSFER_DOC_VERSIONS = ["0.14", "0.15", "0.16", "0.17"]
LAKEXPRESS_DOC_VERSIONS = ["0.2", "0.3", "0.4"]
MIGRATORXPRESS_DOC_VERSIONS = [
    "0.6.27",
    "0.6.28",
    "0.6.29",
    "0.6.30",
    "0.6.31",
    "0.6.32",
]

# Release-notes URL templates ({version} is substituted with e.g. "0.31")
RELEASE_NOTES_URLS = {
    "fastbcp": "https://fastbcp-docs.arpe.io/latest/release-notes/release-notes-{version}",
    "fasttransfer": "https://fasttransfer-docs.arpe.io/latest/release-notes/release-notes-{version}",
    "lakexpress": "https://lakexpress-docs.arpe.io/latest/release-notes/release-notes-{version}",
    "migratorxpress": "https://aetperf.github.io/MigratorXpress-Documentation/release-notes-{version}",
}

DOC_URLS = {
    "fastbcp": "https://fastbcp-docs.arpe.io/{version}/sitemap",
    "fasttransfer": "https://fasttransfer-docs.arpe.io/{version}/sitemap",
    "lakexpress": "https://lakexpress-docs.arpe.io/{version}/sitemap",
    "migratorxpress": "https://aetperf.github.io/MigratorXpress-Documentation/",
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
