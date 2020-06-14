import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="clodss",
    version="0.0.1",
    author="Codoma.tech Team",
    author_email="info@codoma.tech",
    description="On-Disk data-structures store with redis-like API.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/codomatech/clodss",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
