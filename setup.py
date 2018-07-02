from setuptools import setup

setup(
    name='mondrian-rest',
    version='0.4',
    description='Python client for mondrian-rest',
    url='http://github.com/Datawheel/mondrian-rest',
    author='Manuel Aristaran - Datawheel',
    author_email='manuel@jazzido.com',
    license='MIT',
    packages=['mondrian_rest'],
    install_requires=['numpy', 'pandas', 'requests'],
    zip_safe=False)
