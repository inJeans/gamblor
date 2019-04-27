from setuptools import setup

def readme():
    with open("README.md") as f:
        return f.read()

setup(name="gamblor",
      version="0.0.1-alpha",
      description="Package for automated sports predicitons",
      long_description=readme(),
      classifiers=[
        "Development Status :: 1 - Planning",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Natural Language :: English",
        "Programming Language :: Python :: 3",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
      ],
      keywords="sport predictions afl machine learning artificial intelligence",
      url="http://github.com/InJeans/gamblor",
      author="Christopher J Watkins",
      author_email="christopher.watkins@me.com",
      license="MIT",
      packages=["gamblor"],
      install_requires=[
          "luigi",
          "sqlalchemy",
          "pandas",
      ],
      test_suite="nose.collector",
      tests_require=["nose"],
      entry_points = {
        "console_scripts": ["funniest-joke=gamblor.command_line:main"],
      },
      include_package_data=True,
      zip_safe=False)
