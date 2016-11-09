from setuptools import setup

setup(name='mondrian_rest',
      version='0.1',
      description='Python client for mondrian-rest',
      url='http://github.com/jazzido/py-mondrian-rest',
      author='Manuel Aristaran',
      author_email='manuel@jazzido.com',
      license='MIT',
      packages=['mondrian_rest'],
      install_requires=[
          'numpy',
          'pandas',
          'requests'
      ],
      zip_safe=False)
