# zuschauer [![Python 3.7](https://img.shields.io/badge/python-3.7-blue.svg)](https://www.python.org/downloads/release/python-370/) ![t](https://img.shields.io/badge/status-stable-green.svg) [![](https://img.shields.io/github/license/drahnreb/zuschauer.svg)](https://github.com/drahnreb/zuschauer/blob/master/LICENSE.md)
IoT simplified - watchdog + azure storage options

(*Der Zuschauer dt. - spectator*)


![Zuschauer by Bernhard Häußler](/../media/screenshot.png?raw=true "Screenshot of Zuschauer")

## Details
Watch a (or more) specified folder(s) for newly created or modified files and **copy** them to configured storage option. Supported options are `Azure Storage Blob`, `ADLS Gen 2` or on-premise network drives. Azure functionality is implemented by leveraging [Azure Blob Storage Python SDK](https://github.com/Azure/azure-sdk-for-python).
The AzureBlobFileSystem accepts [all of the Async BlobServiceClient arguments](https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-python).
Zuschauer uses official APIs and opens files in read-only byte mode to copy files, it waits a second to prevent data loss.
By default, write operations create BlockBlobs in Azure, which, once written can not be appended.

## Usage
Easiest configuration is via gooey, first configure with via the interface:
```bash
python zuschauer.py
```
Specify required (and optional) arguments in interface.

Test configuration with a flag `--dryrun` to save config file and test connection without actually uploading anything.
```bash
python zuschauer.py --dryrun
```

By default, `zuschauer` is saving a JSON-config file if arguments are correct and connection can be established.
That enables a headless mode: Just run it a second time (after configuration) and it will automatically load all pre-configured details.
Use `--existing` flag to also upload all existing files (only necessary at first time or after interruption).
It will not overwrite already uploaded files.
```bash
python zuschauer.py --existing
```

Zuschauer looks for a .config file in its root. If necessary, refer to any other path with `--load`:
```bash
python zuschauer.py --load 'path/to/config.ajsonfile'
```
Example `config.ajsonfile`:
``` json
{"paths": ["/path/to/watched_folder", "/second/path/to/watched_folder"], "filetypes": "pdf;tex", "storage": "Blob", "proxy": "", "refresh": 1, "recursive": true, "verbose": true, "dryrun": false}
```

Every other option is described via help:
```bash
python zuschauer.py -h
```

## Upcoming features:
* dockerize
* concurrent upload of existing files and zs.run()
* implement on-premise location (file share)

## Author
Bernhard Häußler, TU Berlin

## License
zuschauer is licensed under the MIT license, as included in the [LICENSE](LICENSE) file.

* Copyright (C) 2021 zuschauer contributors

Please see the git history for authorship information.

If not stated elsewise:
Copyright (C) 2019-2021 Bernhard J. Häussler "drahnreb"

gooey:
Copyright (c) 2013-2017 Chris Kiehl

azure sdk:
Copyright (c) 2016 Microsoft
