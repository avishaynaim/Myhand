from setuptools import setup, find_packages

setup(
    name='yad2-monitor',
    version='2.0.0',
    packages=find_packages(),
    include_package_data=True,
    package_data={
        '': ['templates/*.html', 'static/**/*', 'static/*'],
    },
    install_requires=[
        line.strip()
        for line in open('requirements.txt').readlines()
        if line.strip() and not line.startswith('#')
    ],
)
