from setuptools import setup, find_packages

setup(
    name="dataserver",
    version="0.1.0",
    description="Local dataserver package",
    author="Kyle Marino",
    author_email="kyle.marino22@gmail.com",
    python_requires=">=3.8",
    packages=find_packages(),
    install_requires=[
        # e.g. "requests>=2.28", "pandas>=1.5", …
    ],
    entry_points={
        # (optional) if you want to create console‐scripts
        "console_scripts": [
            "ib-dataserver = dataserver.ib_dataserver.main:main",
        ],
    },
)