from setuptools import setup, find_packages

import trading

setup(
  name='trading_package',
  version=trading.__version__,
  author=trading.__author__,
  author_email='tomasz.lesniak@capgemini.com',
  description='trading wheel',
  packages=find_packages(include=['trading']),exclude=['tests'],
  entry_points={
    'group_1': 'run=trading.__main__:main'
  },
  install_requires=[
    'setuptools'
  ]
)
