# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "flotta"
copyright = "2024, IDSIA"
author = "IDSIA"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx_rtd_theme",
    "myst_parser",
]

templates_path = [
    "_templates",
]
exclude_patterns = []

# The suffix(es) of source filenames.
# You can specify multiple suffix as a list of string:
# source_suffix = ['.rst', '.md']
source_suffix = {
    ".rst": "restructuredtext",
    ".txt": "markdown",
    ".md": "markdown",
}

# The master toctree document.
master_doc = "index"

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "sphinx_rtd_theme"
html_static_path = [
    "_static",
]
html_theme_options = {
    "collapse_navigation": False,
    "display_version": True,
    "logo_only": True,
}
html_logo = "_static/images/hero-bg-blue.png"

# -- Options for MyST parse output -------------------------------------------
myst_enable_extensions = [
    "colon_fence",
    "attrs_inline",
]
