from distutils.core import setup

from setuptools import find_packages

setup(name='pyTails',
      version='0.0.3',
      description='Python DB tailer',
      author='Atharva Inamdar',
      packages=find_packages('.'),
      entry_points={
          'console_scripts': ['pytails=pytails.pytails:main'],
      },
      python_requires='>=3.6, <3.8'
      )
