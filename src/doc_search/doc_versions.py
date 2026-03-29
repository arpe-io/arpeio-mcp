"""
Version mappings and URL templates for arpe.io documentation sites.
"""

FASTBCP_DOC_VERSIONS = ["0.28", "0.29", "0.30"]
FASTTRANSFER_DOC_VERSIONS = ["0.14", "0.15", "0.16"]
LAKEXPRESS_DOC_VERSIONS = ["0.2", "0.3"]
MIGRATORXPRESS_DOC_VERSIONS = ["0.6.27"]

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
