# excel2parquet

### Prerequisites
#### Python 3.6.* or later.

See installation instructions at: https://www.python.org/downloads/

Check you have python3 installed:

```bash
python3 --version
```

#### Preferably an IDE such as Pycharm Community Edition

https://www.jetbrains.com/pycharm/download/


### Dependencies and data

#### Creating a virtual environment

Ensure your pip (package manager) is up to date:

```bash
pip3 install --upgrade pip
```

To check your pip version run:

```bash
pip3 --version
```

Create the virtual environment in the root of the cloned project:

```bash
python3 -m venv .venv
```

#### Activating the newly created virtual environment

You always want your virtual environment to be active when working on this project.

```bash
source ./.venv/bin/activate
```

#### Installing Python requirements

This will install some of the packages you might find useful:

```bash
pip3 install -r ./requirements.txt
```

#### Running tests to ensure everything is working correctly

```bash
pytest ./tests
```

####  data

A data generator is included as part of the project in `./input_data_generator/main_data_generator.py`
This allows you to generate a configurable number of months of data.
Although the technical test specification mentions 6 months of data, it's best to generate
less than that initially to help improve the debugging process.

