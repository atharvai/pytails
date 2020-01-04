from distutils.core import setup

setup(name='pyTails',
      version='0.0.2',
      description='Python DB tailer',
      author='Atharva Inamdar',
      packages=['pytails'],
      entry_points={
          'console_scripts': ['pytails=pytails.pytails:main'],
      },
      python_requires='>=3.6, <3.8'
      )
