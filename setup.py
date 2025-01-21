from setuptools import setup, find_packages

setup(
    name="azure-config-collector",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "azure-storage-queue>=12.0.0",
        "azure-identity>=1.12.0",
        "azure-keyvault-secrets>=4.7.0",
        "azure-mgmt-resource>=23.0.0",
        "snowflake-connector-python>=3.0.0",
        "backoff>=2.2.0",
        "python-dotenv>=1.0.0",
        "pydantic>=2.0.0",
        "structlog>=24.1.0",
        "tenacity>=8.2.0"
    ],
    entry_points={
        'console_scripts': [
            'azure-collector=src.main:run_collector',
        ],
    },
    python_requires=">=3.9",
    author="Your Name",
    author_email="your.email@example.com",
    description="Azure Configuration Collector",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.9",
    ],
)