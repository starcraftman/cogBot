"""
The setup file for packaging cogbot
"""
from __future__ import absolute_import, print_function

import fnmatch
import glob
import os
import shlex
import subprocess
import sys
from setuptools import setup, find_packages, Command

ROOT = os.path.abspath(os.path.dirname(__file__))
if os.path.dirname(__file__) == '':
    ROOT = os.getcwd()


SHORT_DESC = 'The Elite Federal Discord Bot'


def rec_search(wildcard):
    """
    Traverse all subfolders and match files against the wildcard.

    Returns:
        A list of all matching files absolute paths.
    """
    matched = []
    for dirpath, _, files in os.walk(os.getcwd()):
        fn_files = [os.path.join(dirpath, fn_file) for fn_file
                    in fnmatch.filter(files, wildcard)]
        matched.extend(fn_files)
    return matched


class CleanCommand(Command):
    """
    Equivalent of make clean.
    """
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        pycs = ' '.join(rec_search('*.pyc'))
        eggs = ' '.join(glob.glob('*.egg-info') + glob.glob('*.egg'))
        cmd = 'rm -vrf .eggs .tox build dist {0} {1}'.format(eggs, pycs)
        print('Executing: ' + cmd)
        if raw_input('OK? y/n  ').strip().lower()[0] == 'y':
            subprocess.call(shlex.split(cmd))


class InstallDeps(Command):
    """
    Install dependencies to run & test.
    """
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        print('Installing runtime & testing dependencies')
        cmd = 'pip install ' + ' '.join(RUN_DEPS + TEST_DEPS)
        print('Will execute: ' + cmd)
        subprocess.call(shlex.split(cmd))


class UMLDocs(Command):
    """
    Generate UML class and module diagrams.
    """
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def check_prereqs(self):
        """
        Checks required programs.
        """
        try:
            with open(os.devnull, 'w') as dnull:
                subprocess.check_call(shlex.split('pyreverse -h'),
                                      stdout=dnull, stderr=dnull)
        except OSError:
            print('Missing pylint library (pyreverse). Please run:')
            print('pip install pylint')
            sys.exit(1)
        try:
            with open(os.devnull, 'w') as dnull:
                subprocess.check_call(shlex.split('dot -V'),
                                      stdout=dnull, stderr=dnull)
        except OSError:
            print('Missing graphviz library (dot). Please run:')
            print('sudo apt-get install graphviz')
            sys.exit(1)

    def run(self):
        self.check_prereqs()
        old_cwd = os.getcwd()
        os.chdir(ROOT)

        cmds = [
            'pyreverse pakit',
            'dot -Tps classes_No_Name.dot -o class_diagram.ps',
            'dot -Tps packages_No_Name.dot -o module_diagram.ps',
        ]
        for cmd in cmds:
            subprocess.call(shlex.split(cmd))
        for fname in glob.glob('*.dot'):
            os.remove(fname)
        print('Diagrams available in: ' + ROOT)
        print('Use any postscript viewer to open them.')

        os.chdir(old_cwd)


MY_NAME = 'Jeremy Pallats / starcraft.man'
MY_EMAIL = 'N/A'
RUN_DEPS = ['argparse', 'discord.py', 'google-api-python-client', 'pyyaml', 'SQLalchemy']
TEST_DEPS = ['coverage', 'flake8', 'mock', 'pytest', 'sphinx', 'tox']
setup(
    name='cogbot',
    version='0.1.0',
    description=SHORT_DESC,
    long_description=SHORT_DESC,
    url='https://github.com/starcraftman/cogBot',
    author=MY_NAME,
    author_email=MY_EMAIL,
    maintainer=MY_NAME,
    maintainer_email=MY_EMAIL,
    license='BSD',
    platforms=['any'],

    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Intended Audience :: End Users/Desktop',
        'License :: OSI Approved :: BSD License',
        'Operating System :: POSIX :: Linux',
        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3 or both.
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],

    # What does your project relate to?
    keywords='development',

    # You can just specify the packages manually here if your project is
    # simple. Or you can use find_packages().
    # packages=find_packages(exclude=['venv', 'test*']),
    packages=find_packages(exclude=['venv', 'tests*']),

    # List run-time dependencies here.  These will be installed by pip when
    # your project is installed. For an analysis of "install_requires" vs pip's
    # requirements files see:
    # https://packaging.python.org/en/latest/requirements.html
    install_requires=RUN_DEPS,

    tests_require=TEST_DEPS,

    # # List additional groups of dependencies here (e.g. development
    # # dependencies). You can install these using the following syntax,
    # # for example:
    # # $ pip install -e .[dev,test]
    extras_require={
        'dev': ['pyandoc'],
        'test': TEST_DEPS,
    },

    # # If there are data files included in your packages that need to be
    # # installed, specify them here.  If using Python 2.6 or less, then these
    # # have to be included in MANIFEST.in as well.
    package_data={
        'cog': ['.secret/config.yaml', '.secrets/sheets.json', '.secrets/sheets.token'],
    },

    # # Although 'package_data' is the preferred approach, in some case you may
    # # need to place data files outside of your packages. See:
    # # http://docs.python.org/3.4/distutils/setupscript.html#installing-additional-files # noqa
    # # In this case, 'data_file' will be installed into '<sys.prefix>/my_data'
    # data_files=[('my_data', ['data/data_file'])],

    # To provide executable scripts, use entry points in preference to the
    # "scripts" keyword. Entry points provide cross-platform support and allow
    # pip to create the appropriate form of executable for the target platform.
    entry_points={
        'console_scripts': [
            'cogbot = cog.bot:main',
            'cogloc = cog.local:main',
        ],
    },

    cmdclass={
        'clean': CleanCommand,
        'deps': InstallDeps,
        'uml': UMLDocs,
    }
)
