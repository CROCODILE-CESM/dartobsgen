# Configuration file for the Sphinx documentation builder.
# https://www.sphinx-doc.org/en/master/usage/configuration.html

from importlib.metadata import PackageNotFoundError, version

# -- Project information -----------------------------------------------------

project = "dartobsgen"
copyright = "2026, University Corporation for Atmospheric Research"
author = "Data Assimilation Research Section"

try:
    release = version("dartobsgen")
except PackageNotFoundError:  # docs built without the package installed
    release = "0.0.0.dev0"
version = release

# -- General configuration ---------------------------------------------------

extensions = [
    "myst_parser",
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.napoleon",
    "sphinx.ext.intersphinx",
    "sphinx.ext.viewcode",
    "sphinx_copybutton",
    "sphinx_design",
]

exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

# -- MyST --------------------------------------------------------------------

myst_enable_extensions = [
    "colon_fence",
    "deflist",
    "substitution",
    "attrs_inline",
]
myst_heading_anchors = 3

# -- autodoc / autosummary ---------------------------------------------------

autosummary_generate = True
autodoc_member_order = "bysource"
autodoc_typehints = "description"
autodoc_default_options = {
    "members": True,
    "undoc-members": False,
    "show-inheritance": True,
}

# dartobsgen imports only numpy, pandas and shapely at module scope; the
# heavier dependencies (nnja, pydartdiags, xarray, gsw, dask, f90nml) are
# imported lazily inside the functions that use them, so the docs build does
# not need them.  Keep this list in sync if a lazy import is ever hoisted to
# module scope.
autodoc_mock_imports = []

# The docstrings are NumPy style.
napoleon_google_docstring = False
napoleon_numpy_docstring = True
napoleon_use_param = True
napoleon_use_rtype = True
# Docstrings write bare `datetime`/`timedelta`; point them at the stdlib docs.
napoleon_type_aliases = {
    "datetime": ":py:class:`~datetime.datetime`",
    "timedelta": ":py:class:`~datetime.timedelta`",
    "Polygon": ":py:class:`~shapely.Polygon`",
}

# -- intersphinx -------------------------------------------------------------

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "numpy": ("https://numpy.org/doc/stable", None),
    "pandas": ("https://pandas.pydata.org/docs", None),
    "xarray": ("https://docs.xarray.dev/en/stable", None),
    "shapely": ("https://shapely.readthedocs.io/en/stable", None),
}

# -- HTML output -------------------------------------------------------------

html_theme = "pydata_sphinx_theme"
html_static_path = ["_static"]
html_css_files = ["custom.css"]
html_title = "dartobsgen"
html_show_sourcelink = False
html_use_index = False

html_theme_options = {
    "github_url": "https://github.com/CROCODILE-CESM/dartobsgen",
    "icon_links": [
        {
            "name": "DART documentation",
            "url": "https://docs.dart.ucar.edu/en/latest/",
            "icon": "fa-solid fa-book",
        },
    ],
    "navbar_start": ["navbar-logo"],
    "navbar_align": "content",
    "show_toc_level": 2,
    "show_nav_level": 1,
    "use_edit_page_button": True,
    "footer_start": ["copyright"],
    "footer_end": ["theme-version"],
    "announcement": "Welcome to the dartobsgen documentation! 🚀",
}

html_context = {
    "github_user": "CROCODILE-CESM",
    "github_repo": "dartobsgen",
    "github_version": "main",
    "doc_path": "docs",
    "default_mode": "auto",
}

# Warn about references that cannot be resolved (the docs CI builds with -W).
nitpicky = False
