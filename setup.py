# setup.py

from setuptools import setup, find_packages

setup(
    name="monitor_oracle_fts",
    version="0.1",
    description="Projeto modular para monitorar Full Table Scans (FTS) no Oracle 12C",
    author="Seu Nome",
    author_email="seu@email.com",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "cx_Oracle>=8.0",
        "python-dotenv>=0.19",
        "GitPython>=3.1",
        "sql-metadata>=2.4"
    ],
    entry_points={
        "console_scripts": [
            "run-monitor=main:main"
        ]
    },
)