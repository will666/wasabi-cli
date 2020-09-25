# wasabi-cli
The app is composed of 4 modules that can run independently but using the [CLI](manage/src/cli.py) is the main approch.

## Modules list
[manage](manage), [lambda/main](lambda/main.py), [lambda/encoder](lambda/encoder.py), [layer/ffmpeg/build](lambda/layer/ffmpeg/build.py)

Each module holds its own `Pipfile` and python3 environment.

## Root Installation (optional)
The environment at the root directory is dedicated to code formatting and is optional.
```shell
pipenv --three
pipenv shell
pipenv install --dev
pipenv run dev-setup
```

## Per project installation
> NOTE: exit active virtual environment to achieve per module install.

```shell
cd [MODULE]
pipenv --three
pipenv shell
pipenv install --dev
```

## Run
The [CLI](manage/cli.py) is a wrapper for the app core [manage/src/main.py](manage/src/main.py):

```shell
cd manage/src
python3 cli.py
```

Core [manage](manage) module can be used as a standalone app:
```shell
cd manage/src
python3 main.py
```

## Build Lambda layer and deploy to S3
The layer will be uploaded to S3 as specified in [config.py](lambda/layer/ffmpeg/config.py) file.
```shell
cd lambda/layer/ffmpeg
python3 build.py
```

## Cloud configuration

- [Lambda](lambda) module to be added to S3 events property of the [designated](manage/.env) S3 bucket.

- Lambda [layer](lambda/layer/ffmpeg/package/ffmpeg_lambda_layer.zip) to be created and connected to [Lambda](lambda) function.

## License
[MIT](LICENSE)