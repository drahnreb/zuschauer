# zuschauer
IoT simplified - watchdog + azure storage blob

## Usage
Easiest configuration is via gooey, first configure with via the interface:
```bash
python zuschauer.py
```
Specify required (and optional) arguments in interface.


By default, `zuschauer` is saving a JSON-config file if arguments are correct and connection can be established.
That enables a headless mode: Just run it a second time (after configuration) and it will automatically load all pre-configured details.
```bash
python zuschauer.py
```
__Use with caution__:
This saves also credentials on disk.

Zuschauer looks for a .config file in its root. If necessary, refer to any other path with `--load`:
```bash
python zuschauer.py --load 'path/to/config.ajsonfile'
```

Use `--dryrun` option to save config file and test connection without actually uploading anything.
```bash
python zuschauer.py --dryrun
```

Every other option is described via help:
```bash
python zuschauer.py -h
```

## Upcoming features:
* concurrent upload of existing files and zs.run()
* implement on-premise location (file share)
