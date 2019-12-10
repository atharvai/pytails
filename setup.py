from distutils.core import setup

setup(name='pyTails',
      version='0.0.1',
      description='Python DB tailer',
      author='Atharva Inamdar',
      packages=['pytails'],
      scripts=['pytails.py'],
      python_requires='>=3.6, <3.8'
      )
