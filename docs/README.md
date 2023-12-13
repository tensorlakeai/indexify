# Serve Docs

### Create virtualenv
cd into this directory
make a virtual environment
install requirements
```bash
cd docs
virtualenv -p python3 ve && source ve/bin/activate
pip install -r requirements.txt
```

### Serve docs
```bash
mkdocs serve
```