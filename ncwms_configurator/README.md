# ncWMS configurator

Queries the metadata database and creates an ncWMS config file from the results

## Installation

```bash
virtualenv venv
source venv/bin/activate
pip install -U pip
pip install -r requirements.txt -i http://tools.pacificclimate.org/pypiserver/ --trusted-host tools.pacificclimate.org
```

## Usage

Built in help is provided: `python ncwms_configurator.py -h`