from setuptools import setup, find_packages

# Read the contents of your README file
from pathlib import Path
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

setup(
    name="injective-trader",
    use_scm_version=True,
    setup_requires=["setuptools-scm"],
    author="Runxue Yu",
    author_email="runxue@injectivelabs.org",
    description="A comprehensive SDK for building trading strategies on Injective Protocol",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/InjectiveLabs/injective-trader",
    packages=find_packages(),
    python_requires=">=3.9",
    install_requires=[
        "injective-py==1.9.0",
        "asyncio-mqtt>=0.16.1",
        "grpcio>=1.54.0",
        "grpcio-tools>=1.54.0",
        "protobuf>=4.23.0",
        "pydantic>=2.0.0",
        "pyyaml>=6.0",
        "python-dotenv>=1.0.0",
        "tabulate>=0.9.0",
        "pandas>=2.0.0",
        "numpy>=1.24.0",
        "redis>=4.5.0",
        "valkey>=5.0.0",
        "requests>=2.31.0",
        "urllib3>=2.0.0",
        "python-dateutil>=2.8.0",
        "cryptography>=41.0.0",
        "websockets>=11.0.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-asyncio>=0.21.0",
            "pytest-cov>=4.0.0",
            "black>=23.0.0",
            "isort>=5.12.0",
            "flake8>=6.0.0",
            "mypy>=1.4.0",
            "pre-commit>=3.0.0",
        ],
    },
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
    entry_points={
        "console_scripts": [
            "injective-trader=injective_trader.cli:main",
        ],
    },
    include_package_data=True,
    zip_safe=False,
)