import sys
import alabaster
import pkg_resources

extensions = [
    'alabaster',
    'sphinx.ext.autodoc',
    'sphinx.ext.intersphinx',
    'sphinx.ext.viewcode',
    'sphinx.ext.napoleon'
]

templates_path = ['_templates']
source_suffix = '.rst'

master_doc = 'index'

project = 'bloop'
copyright = '2016, Joe Cross'
author = 'Joe Cross'


try:
    release = pkg_resources.get_distribution('bloop').version
except pkg_resources.DistributionNotFound:
    print('To build the documentation, The distribution information of bloop')
    print('Has to be available.  Either install the package into your')
    print('development environment or run "setup.py develop" to setup the')
    print('metadata.  A virtualenv is recommended!')
    sys.exit(1)
del pkg_resources
version = '.'.join(release.split('.')[:2])

language = 'en'

exclude_patterns = ['_build']

pygments_style = 'sphinx'

html_theme = 'alabaster'

html_theme_options = {
    'github_user': 'numberoverzero',
    'github_repo': 'bloop',
    'github_banner': True,
    'travis_button': True,
    'show_powered_by': False,
    'analytics_id': 'UA-65843067-1'
}
html_theme_path = [alabaster.get_path()]
html_static_path = ['_static']
html_sidebars = {
    '**': [
        'about.html',
        'navigation.html',
        'relations.html',
        'searchbox.html'
    ]
}

intersphinx_mapping = {
    'python': ('https://docs.python.org/3.6', None),
    'arrow': ('https://arrow.readthedocs.io/en/latest/', None)
}
