# Python Pandas & PyBuilder

- [Pandas](https://pandas.pydata.org)
- [PyBuilder](https://pybuilder.io)

## Install GCS
```
# Install Google-Cloud-SDK
$ brew install --cask google-cloud-sdk

# Verify installation
$ gcloud version

# Authenticate Google Cloud Developer account
$ gcloud auth login

# Verify account setup
$ gcloud auth list
$ gcloud config list
```

## Install Python Packages
```
$ pip install -r requirements.txt

# Install PyBuilder
$ pip install pybuilder
```

## Play with PyBuilder
```
# Build
$ pyb clean publish

# Install
$ cd target/dist/app-1.0.dev0/dist
$ pip install app-1.0.dev0.tar.gz

# Run
$ cd target/dist/app-1.0.dev0/scripts
$ python run_etl.py

# Uninstall
$ pip uninstall app
```

# Run at once
```
$ pyb clean publish && pip install target/dist/app-1.0.dev0/dist/app-1.0.dev0.tar.gz && python target/dist/app-1.0.dev0/scripts/run_etl.py
```
