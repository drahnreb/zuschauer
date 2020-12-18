# zuschauer
IoT simplified - watchdog + azure storage blob

![Zuschauer by Bernhard Häußler](/../media/screenshot.png?raw=true "Screenshot of Zuschauer")

## Function
Watch (*Der Zuschauer dt. - spectator*) a (or more) specified folder(s) for newly created or modified files and **copy** them to configured storage option. Supported options are `Azure Storage Blob`, `ADLS Gen 1` (untested) or on-premise Network Drives (in future).
Zuschauer uses official APIs and opens files in read-only byte mode to copy files, it waits a second to prevent data loss.

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
__Use with caution__:
This saves also credentials on disk.

Zuschauer looks for a .config file in its root. If necessary, refer to any other path with `--load`:
```bash
python zuschauer.py --load 'path/to/config.ajsonfile'
```

Every other option is described via help:
```bash
python zuschauer.py -h
```

## Upcoming features:
* concurrent upload of existing files and zs.run()
* implement on-premise location (file share)
* fix localization


## Author
Bernhard Häußler, TU Berlin