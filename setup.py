from setuptools import setup, find_packages

setup(
    name="titan-tools",
    version="1.0.0",
    description="Shared core library for Titan automation tools",
    author="Ashwin Nair",
    packages=find_packages(),
    python_requires=">=3.8",
    entry_points={
        "console_scripts": [
            "titan_tools = titan_tools.__main__:main"
        ],
    },
)
