from setuptools import setup, find_packages

setup(
    name="binance-tick",
    version="0.0.1",
    url="https://github.com/xzmeng/binance-tick",
    author="Meng Xiangzhuo",
    author_email="aumo@foxmail.com",
    description="Get binance historical tick data.",
    packages=find_packages(),
    install_requires=["requests", "pandas"],
)
