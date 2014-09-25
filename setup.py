from setuptools import setup

setup(name='dropboxfs',
      version='0.0.0',
      description='A simple FUSE-based file system for Dropbox',
      author='Miguel Branco',
      author_email='miguel.branco@epfl.ch',
      license='MIT License',
      packages=['dropboxfs'],
      scripts=['dropboxfs/dropboxfs'],
      )
