from setuptools import setup, find_packages


def parse_requirements(filename):
    with open(filename, 'r') as file:
        lines = file.readlines()
    return [line.strip() for line in lines if line.strip() and not line.startswith('#')]


if __name__ == '__main__':
    setup(
        name='baseball_stats',
        version='2.1.1',
        packages=find_packages(),
        install_requires=parse_requirements('requirements.txt'),
        author='Bryce Dickson',
        description='Backend management for a baseball stats db',
        url='https://github.com/Bla005-1/baseball_stats_backend',
        classifiers=[
            'Programming Language :: Python :: 3',
            'Operating System :: OS Independent',
        ],
        python_requires='>=3.10',
    )
