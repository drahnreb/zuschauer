#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = "Bernhard Häußler"
__copyright__   = "Copyright (c) 2020"
__version__ = 0.4
__license__ = "MIT"
__maintainer__ = "Bernhard Häußler"
__email__ = "@drahnreb"
__status__ = "Production"

"""
    Zuschauer (*der Zuschauer dt. - spectator*) - 
    Watch a (or more) specified folder(s) for newly created or modified files and **copy** them to configured storage option.
    Supported options are `Azure Storage Blob`, `ADLS Gen 2`, on-premise Network Drives or MQTT Topics.
    Zuschauer uses official APIs and opens files in read-only byte mode to copy files, it waits a second to prevent data loss.
    You need to install pip install pywin32.
    After that you need to run python Scripts/pywin32_postinstall.py -install from your Python directory to register the dlls.
    To hide the program, you can run it via pythonw.exe.

"""

# only when in "azure mode", zuschauer is using official Azure SDK for python
# https://github.com/Azure/azure-sdk-for-python/blob/master/sdk/eventhub/azure-eventhub-checkpointstoreblob-aio/azure/eventhub/extensions/checkpointstoreblobaio/_vendor/storage/blob/_blob_client.py
# https://github.com/Azure/azure-sdk-for-python/tree/master/sdk/storage/azure-storage-blob

if __package__ is None or __package__ == '':
    from Gooey.gooey import Gooey, GooeyParser
import argparse
from pathlib import Path
import os
import platform
import sys
import shutil
import time
import logging
import tempfile
from watchdog.observers import Observer
from watchdog.events import LoggingEventHandler
from watchdog.events import FileSystemEventHandler
import re
import arrow
import subprocess
import sys
import json

import keyring
STORECREDENTIALS = False
try:
    if platform.system().lower().startswith("win"):
        import win32com.client # pywin32
        STORECREDENTIALS = True
    elif platform.system().lower().startswith("lin"):
        import secretstorage
        STORECREDENTIALS = True
    else:
        raise NotImplementedError
except ImportError as e:
    if platform.system().lower().startswith("win"):
        try:
            from win32ctypes.pywin32 import win32cred
            win32cred.__name__
        except ImportError as e:
            print(
                """
                    You need to install pip install pywin32 or pywin32-ctypes.
                    After that you need to run python Scripts/pywin32_postinstall.py -install from your Python directory to register the dlls.
                """
            )
    else:
        print("Cannot use keyring features. Won't be able to store credentials", str(e))

MQTTBUFSIZE = 2000
STORAGES = ["Blob", "ADLS Gen2", "onPrem", "MQTT"]
CONFIGFILE = Path(Path(__file__).absolute().parent).joinpath('.config')
PAUSEAFTERMODIFIED = 3 # seconds of pause after file modification and until copying starts

@Gooey(program_name="zuschauer @drahnreb", default_size=(800,500), taskbar=True)
def parse_arguments(defaults):
    # use arg parsing without gooey to enable help and enable/disable control of config loading
    # gooey parameter disables 'required arguments' to pass first headless check for load arg
    parser = GooeyParser(description=f'Zuschauer - Filesystem watchdog to copy data to remote storage and enable IoT.\tby {__author__}\tv.{__version__}')
    
    requiredNamed = parser.add_argument_group('Required arguments')
    requiredNamed.add_argument(
        "-paths",
        "-z",
        type=str,  # lambda p: Path(p),
        default=[str(Path(__file__).resolve().parent)],
        nargs='+',
        help="Zuschauer root path(s); watched path(s)",
        required=True,
        gooey_options={
            'initial_value': defaults.get('paths', [str(Path(__file__).resolve().parent)])  
        }
    )
    requiredNamed.add_argument(
        "-filetypes",
        "-f",
        default='',
        required=True,
        help="Allowed file suffix(es) (e.g. .pdf or txt), semicolon-separated (e.g. .pdf;txt). Use asterisk (*) for all types.",
        gooey_options={
            'initial_value': defaults.get('filetypes', '')  
        }
    )
    requiredNamed.add_argument(
        "-storage_type",
        "-t",
        default=STORAGES[0],
        choices=STORAGES,
        required=True,
        help="Storage Option.",
        gooey_options={
            'initial_value': defaults.get('storage_type', STORAGES[0])  
        }
    )
    requiredNamed.add_argument(
        "-destination",
        "-d",
        required=True,
        help='Destination. CHECK CAREFULLY!   if MQTT: Topic (no spaces;only ASCII is enforced!) // if onPrem: Network Share Path // if Azure Storage Blob: Container Name if Azure Storage ADLS Gen2: Filesystem',
        gooey_options={
            'initial_value': defaults.get('destination', "")  
        }
    )
    # optional
    parser.add_argument(
        "--account_name",
        "-n",
        default='',
        help='Azure Storage Identity: AccountName (from portal.azure.com) // MQTT: Broker Hostname/IP',
        gooey_options={
            'initial_value': defaults.get('account_name', "")  
        }
    )
    parser.add_argument(
        "--account_key",
        "-k",
        default='',
        help='Azure Storage Identity: Account Key (aka TenantID when Service Principal credentials) // MQTT: Broker Port',
        gooey_options={
            'initial_value': defaults.get('account_key', "")  
        }
    )
    parser.add_argument(
        "--client_id",
        "-i",
        default='',
        help='Azure Storage Identity (only required if Service Principal): Client ID',
        gooey_options={
            'initial_value': defaults.get('client_id', "")  
        }
    )
    parser.add_argument(
        "--client_secret",
        "-c",
        default='',
        help='Azure Storage Identity (only required if Service Principal): Client Secret',
        gooey_options={
            'initial_value': defaults.get('client_secret', "")  
        }
    )
    parser.add_argument(
        "--proxy",
        "-p",
        default='',
        help="Semicolon separated Proxy URLs or IP Adresses for http;http(s) if proxy doesn't support https use http:// prefix twice\nformat: 'http://proxyURLorIP:proxyPort;http(s)://proxyURLorIP:proxyPort'",
        gooey_options={
            'initial_value': defaults.get('proxy', "")  
        }
    )
    parser.add_argument(
        "--ssl_verify",
        action='store_true',
        default=False,
        help="En-/Disable SSL Certificate Verification.",
        gooey_options={
            'initial_value': defaults.get('ssl_verify', False)  
        }
    )
    parser.add_argument(
        "--save",
        action='store_true',
        default=True,
        help="Save JSON config for next startup or headless mode. (Credentials are stored in keyring)",
        gooey_options={
            'initial_value': defaults.get('save', True)  
        }
    )
    parser.add_argument(
        "--load",
        default=CONFIGFILE,
        type=str,  # lambda p: Path(p),
        help="Specify path to JSON config file that should be used and loaded",
        gooey_options={
            'initial_value': defaults.get('load', CONFIGFILE)  
        }
    )
    parser.add_argument(
        "--refresh",
        type=int,
        default=1,
        help="Refresh Frequency.",
        gooey_options={
            'initial_value': defaults.get('refresh', 1)  
        }
    )
    parser.add_argument(
        "--recursive",
        action='store_true',
        default=True,
        help="Enable nested paths (deep changes) and check root paths recursively.",
        gooey_options={
            'initial_value': defaults.get('recursive', True)  
        }
    )
    parser.add_argument(
        "--verbose",
        action='store_true',
        default=True,
        help="Run in verbose mode.",
        gooey_options={
            'initial_value': defaults.get('verbose', True)  
        }
    )
    parser.add_argument(
        "--dryrun",
        action='store_true',
        default=False,
        help="Use as a dry run to save config file and test connection without actually uploading anything. E.g. use to create JSON config file only.",
        gooey_options={
            'initial_value': defaults.get('dryrun', False)  
        }
    )
    parser.add_argument(
        "--existing",
        action='store_true',
        default=True,
        help="Upload existing files in specified paths.",
        gooey_options={
            'initial_value': defaults.get('existing', True)  
        }
    )
    parser.add_argument(
        "--pipeline",
        action='store_true',
        default=True,
        help="Wait delay to prevent azure data factory pipeline concurrency failure.",
        gooey_options={
            'initial_value': defaults.get('pipeline', True)  
        }
    )
    parser.add_argument(
        "--reset",
        action='store_true',
        default=False,
        help="Reset all configs.",
        gooey_options={
            'initial_value': defaults.get('reset', False)  
        }
    )
    try:
        return parser.parse_args()
    except SystemExit as e:
        # exit child
        os._exit(e.code)

def _parse_arguments(defaults={}, gooey=False):
    # use arg parsing without gooey to enable help and enable/disable control of config loading
    # gooey parameter disables 'required arguments' to pass first headless check for load arg
    parser = argparse.ArgumentParser(description=f'Zuschauer - Filesystem watchdog to copy data to remote storage and enable IoT.\tby {__author__}\tv.{__version__}')
    
    requiredNamed = parser.add_argument_group('Required arguments')
    requiredNamed.add_argument(
        "-paths",
        "-z",
        type=str,  # lambda p: Path(p),
        default=[str(Path(__file__).resolve().parent)] if gooey else None,
        nargs='+',
        help="Zuschauer root path(s); watched path(s)",
        required=gooey
    )
    requiredNamed.add_argument(
        "-filetypes",
        "-f",
        default='' if gooey else None,
        required=gooey,
        help="Allowed file suffix(es) (e.g. .pdf or txt), semicolon-separated (e.g. .pdf;txt). Use asterisk (*) for all types.",
    )
    requiredNamed.add_argument(
        "-storage_type",
        "-t",
        default=STORAGES[0] if gooey else None,
        choices=STORAGES,
        required=gooey,
        help="Storage Option.",
    )
    requiredNamed.add_argument(
        "-destination",
        "-d",
        required=gooey,
        help='Destination. CHECK CAREFULLY!   if MQTT: Topic (no spaces;only ASCII is enforced!) // if onPrem: Network Share Path // if Azure Storage Blob: Container Name if Azure Storage ADLS Gen2: Filesystem',
    )
    # optional
    parser.add_argument(
        "--account_name",
        "-n",
        default='' if gooey else None,
        help='Azure Storage Identity: AccountName (from portal.azure.com) // MQTT: Broker Hostname/IP',
    )
    parser.add_argument(
        "--account_key",
        "-k",
        default='' if gooey else None,
        help='Azure Storage Identity: Account Key (aka TenantID when Service Principal credentials) // MQTT: Broker Port',
    )
    parser.add_argument(
        "--client_id",
        "-i",
        default='' if gooey else None,
        help='Azure Storage Identity (only required if Service Principal): Client ID',
    )
    parser.add_argument(
        "--client_secret",
        "-c",
        default='' if gooey else None,
        help='Azure Storage Identity (only required if Service Principal): Client Secret',
    )
    parser.add_argument(
        "--proxy",
        "-p",
        default='' if gooey else None,
        help="Semicolon separated Proxy URLs or IP Adresses for http;http(s) if proxy doesn't support https use http:// prefix twice\nformat: 'http://proxyURLorIP:proxyPort;http(s)://proxyURLorIP:proxyPort'",
    )
    parser.add_argument(
        "--ssl_verify",
        action='store_true',
        default=False if gooey else None,
        help="En-/Disable SSL Certificate Verification.",
    )
    parser.add_argument(
        "--save",
        action='store_true',
        default=True if gooey else None,
        help="Save JSON config for next startup or headless mode. (Credentials are stored in keyring)",
    )
    parser.add_argument(
        "--load",
        default=CONFIGFILE,
        type=lambda p: Path(p),
        help="Specify path to JSON config file that should be used and loaded",
    )
    parser.add_argument(
        "--refresh",
        type=int,
        default=1 if gooey else None,
        help="Refresh Frequency.",
    )
    parser.add_argument(
        "--recursive",
        action='store_true',
        default=True if gooey else None,
        help="Enable nested paths (deep changes) and check root paths recursively.",
    )
    parser.add_argument(
        "--verbose",
        action='store_true',
        default=True if gooey else None,
        help="Run in verbose mode.",
    )
    parser.add_argument(
        "--dryrun",
        action='store_true',
        default=False if gooey else None,
        help="Use as a dry run to save config file and test connection without actually uploading anything. E.g. use to create JSON config file only.",
    )
    parser.add_argument(
        "--existing",
        action='store_true',
        default=True if gooey else None,
        help="Upload existing files in specified paths.",
    )
    parser.add_argument(
        "--pipeline",
        action='store_true',
        default=True if gooey else None,
        help="Wait delay to prevent azure data factory pipeline concurrency failure.",
    )
    parser.add_argument(
        "--reset",
        action='store_true',
        default=False if gooey else None,
        help="Reset all configs.",
    )
    try:
        if gooey:
            return parser.parse_args()
        else:
            return parser
    except SystemExit as e:
        # exit child
        os._exit(e.code)


def checkArgs(args):
    # check Namespace
    try:
        _ = [args.paths, args.filetypes, args.account_name, args.account_key, args.client_id, args.client_secret,\
            args.destination, args.storage_type, args.proxy, args.ssl_verify, \
            args.save, args.refresh, args.recursive, args.verbose, args.dryrun, args.existing]
    except AttributeError as e:
        print(f"Argument in config not set correctly: \n{e}")
        loggin.error(f"Argument in config not set correctly: \n{e}")
        exit(1)

    # check rest of required args
    if not len(args.paths) or not isinstance(args.paths, list):
        print(f"Zuschauer paths `{args.paths}` not set correctly.")
        logging.error(f"Zuschauer paths `{args.paths}` not set correctly.")
        exit(1)
    else:
        for p in args.paths:
            try:
                assert Path(p).is_absolute()
            except:
                print(f"{p} is not a valid path on this system. Provide an absolute path.")
                logging.ERROR(f"{p} is not a valid path on this system. Provide an absolute path.")
                exit(1)

    # Destination
    assert len(destination), (
        "No Destination Path or Topic set.")

    if not len(args.filetypes):
        print(f"{args.filetypes} not set correctly.")
        logging.error(f"{args.filetypes} not set correctly.")
        exit(1)

    # get proxy settings
    if len(args.proxy) and ';' in args.proxy:
        http_proxy, https_proxy = args.proxy.split(';', 1)
        proxy = dict(http_proxy=http_proxy, https_proxy=https_proxy)
    else:
        proxy = None

    # init storageService
    # check if correct credentials arg is correct to be passed to watchdog
    storageService = StorageService(
        account_name=args.account_name, account_key=args.account_key, client_id=args.client_id,
        client_secret=args.client_secret, destination=args.destination, storage_type=args.storage_type,
        proxy=proxy, ssl_verify=args.ssl_verify
    )

    return storageService


class StorageService():
    def __init__(self,
            account_name: str,
            account_key: str,  # tenant_id
            client_id: str,
            client_secret: str,
            destination: str,
            storage_type: str,
            proxy: dict=None,
            ssl_verify: bool=False):

        self.account_name = account_name
        self.account_key = account_key  # tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.storage_type = storage_type
        self.ssl_verify = ssl_verify
        self.proxy = proxy

        if self.storage_type == "onPrem":
            self.destination = Path(destination).resolve()
        elif self.storage_type == "MQTT":
            # remove reserved $ topic and do not create an unnecessary
            # topic level with a zero character at the front
            destination = destination.replace('$', '').startswith('/', '')
            # remove non ascii compatible and strip whitespaces
            clean_destination = str(destination).strip().replace(' ', '').encode("ascii", "ignore").decode()
            if destination != clean_destination:
                print(f"IMPORTANT! Specified destination contained whitespaces or non ascii compatible characters,\
                        changed Topic to {clean_destination}")
            self.destination = clean_destination
        else:
            # cloud destination. take as is. do not resolve no path obj
            self.destination = destination
    
        self.service_client = None

        # init storage service client
        if self.storage_type == "onPrem":
            self.service_client = self.destination
        elif self.storage_type == "MQTT":
            # service connection
            assert all([self.account_name, self.account_key]), (
                "For MQTT Service, at least Account Name (HOST) and Account Key (PORT) must be specified.")
            self.service_client = mqtt.Client()
            def __on_publish(client, userdata, mid):
                client.mid_value = mid
                client.puback_flag = True
            self.service_client.on_publish = __on_publish
            self.service_client.puback_flag = False  # use flag in publish ack
            self.service_client.mid_value = None
            try:
                # connect to broker
                self.service_client.connect(
                    host=self.account_name,
                    port=self.account_key,
                    60
                )
                # non-blocking threaded interface to the network loop
                self.service_client.loop_start()
            except ConnectionRefusedError:
                self.service_client = None
        else:
            # cloud service
            assert all([self.account_name, self.account_key]), (
                "For Azure Storage Service, at least Account Name and Account Key/Tenant ID must be specified")
            # optional service principal specified
            if self.client_id and self.client_secret:
                # in this case we treat account_key as tenant_id
                self.service_principal = True
            else:
                self.service_principal = False

            # Instantiate service client
            if self.service_principal:
                print("Provided ClientID and ClientSecret. Using Service Principal authentificaion method...")
                self.creds = ClientSecretCredential(
                    tenant_id=self.account_key,
                    client_id=self.client_id,
                    client_secret=self.client_secret,
                    connection_verify=self.ssl_verify
                )
                if self.storage_type == "ADLS Gen2":
                    self.service_client = DataLakeServiceClient(
                        account_url=f"https://{self.account_name}.dfs.core.windows.net",
                        credential=self.creds,
                        connection_verify=self.ssl_verify
                    )
                else:
                    # blob
                    self.service_client = BlobServiceClient(
                        account_url=f"https://{self.account_name}.blob.core.windows.net",
                        credential=self.creds,
                        connection_verify=self.ssl_verify
                    )
            else:
                # using a connection string
                self.connString = f"AccountName={self.account_name};AccountKey={self.account_key}"
                self.config = create_configuration(storage_sdk='blob') # blob
                if self.storage_type == "ADLS Gen2":
                    self.service_client = DataLakeServiceClient.from_connection_string(
                        self.connString,
                        _configuration=self.config,
                        connection_verify=self.ssl_verify
                    )
                else:
                    # blob
                    self.service_client = BlobServiceClient.from_connection_string(
                        self.connString,
                        _configuration=self.config,
                        connection_verify=self.ssl_verify
                    )

                # set proxy policy
                if self.proxy is not None and self.proxy.get('https_proxy') is not None:
                    if self.service_principal:
                        self.service_client._config.proxy_policy = ProxyPolicy(proxies=proxy)
                    else:
                        self.config.proxy_policy.proxies = self.proxy

        if self.service_client is None or not self.connected:
            print(f"A connection to {'mqtt broker' if self.storage_type == 'MQTT' else 'storage option'} could not be established.")
            logging.error("A connection could not be established.")
            exit(1)

    def _get_obj_client(self, fname):
        if self.storage_type == "ADLS Gen2":
            # derive a new file client
            obj_client = self.service_client.get_file_client(file_system=self.destination, file_path=fname)
        elif self.storage_type == "Blob":
            # derive a new blob client
            obj_client = self.service_client.get_blob_client(container=self.destination, blob=fname)
        return obj_client

    def upload(self, input_path: Path, overwrite: bool=False, asynced: bool=False):
        failed = True
        input_path = input_path.resolve()
        if input_path.exists() and input_path.is_file():
            try:
                if self.storage_type in ["Blob", "ADLS Gen2"]:
                    # Instantiate a new Object Client
                    with self._get_obj_client(input_path.name) as obj_client:
                        # Upload content to Storage Account
                        with open(input_path, "rb") as data:
                            if self.storage_type == "ADLS Gen2":
                                obj_client.upload_data(data, length=None, overwrite=overwrite, logging_enable=True)
                            else:
                                # "Blob"
                                obj_client.upload_blob(data, blob_type="BlockBlob", overwrite=overwrite, logging_enable=True)
                    failed = False

                elif self.storage_type == "onPrem":
                    if not overwrite and self.destination.joinpath(input_path.name).exists():
                        # exists, don't copy.
                        # Let it fail to signal no copy was made
                        # failed = False
                        pass
                    else:
                        # copy2 takes src file and output folder and infers filename from source if provided a file
                        shutil.copy2(str(input_path), str(self.destination))
                        failed = False
                else:
                    # hashes
                    out_hash = hashlib.sha256()
                    def build_payload(counter, input_path, chunk=None, out_hash=None):
                        payload = str(counter) + input_path + ","
                        if out_hash is not None:
                            payload += out_hash.hexdigest()
                        payload = bytearray(payload, "utf-8")
                        if chunk is not None:
                            payload += chunk
                        return payload

                    with open(input_path, "rb") as f:
                        n = 0
                        for chunk in f.read(MQTTBUFSIZE):
                            out_hash.update(chunk)
                            self.service_client.publish(
                                topic=self.destination,
                                payload=build_payload(n, input_path, chunk),
                                qos = 1
                            )
                            n += 1
                        else:
                            self.service_client.publish(
                                topic=self.destination,
                                payload=construct_end('end', input_path, out_hash),
                                qos = 1
                            )
            finally:
                pass
            return failed
        else:
            print(f"{input_path} does not exist or not a file.")
            logging.error(f"{input_path} does not exist or not a file.")
        return failed

    def _available_containers(self):
        success = False
        containers = []
        try:
            if self.storage_type == "ADLS Gen2":
                containers = list(self.service_client.list_file_systems(logging_enable=True))
            elif self.storage_type == "Blob":
                containers = list(self.service_client.list_containers(logging_enable=True))
            elif self.storage_type == "MQTT":
                failed, _ = self.service_client.publish(
                    self.destination, out_message,qos)
                if failed:
                    raise ConnectionError
            else:
                # check write permission and folders
                if os.access(self.destination, os.W_OK):
                    containers = os.listdir(self.destination)
                else:
                    raise IOError("Directory not writeable.")
            success = True
        except BaseException as e:
            print(e)
            pass
        return success, containers

    @property
    def connected(self):
        return self._available_containers()[0]


class Zuschauer(FileSystemEventHandler):
    # files to exclude from being watched
    exclude = re.compile(r'|'.join(r'(.+/)?'+ a for a in [
        # Vim swap files
        r'\..*\.sw[px]*$',
        # backup files
        r'.~$',
        # git directories
        r'\.git/?',
        # __pycache__ directories
        r'__pycache__/?',
        ]))

    def __init__(self, paths, filetypes, storage_type, storageService,
            recursive=True, refreshFrequency=1,
            verboseMode=True, dryRun=False
        ):
        self.paths = paths
        self.filetypes = filetypes
        self.storage_type = storage_type
        self.recursive = recursive
        self.dryRun = dryRun
        self.verboseMode = verboseMode if not self.dryRun else True
        self.refreshFrequency = refreshFrequency
        self.storageService = storageService
        self.observer = Observer(timeout=0.1)

        for p in self.paths:
            if p.exists():
                # Add directory
                self.observer.schedule(self, p, recursive=True)

    def execAction(self, changedFile: Path, overwrite: bool):
        if self.verboseMode:
            print_message = arrow.now().format('YYYY-MM-DD HH:mm:ss ZZ')
            print_message += "\t'" + str(changedFile.name) + "'"
            print_message += f"\t{'copy to' if not overwrite else 'overwrite in'} '" + self.storage_type + f"': {self.storageService.destination}"
            print('==> ' + print_message + ' <==')

        # if dryRun active do not execute
        if self.dryRun:
            # sleep 3 secs to emulate long upload
            time.sleep(3)
            print(f"## would have {'copied' if not overwrite else 'overwritten'}.\nbut --dryrun enabled; no action executed.")
            return

        failed = True
        failed = self.storageService.upload(input_path=changedFile, overwrite=overwrite)

        print(f"$$ Successfully {'copied' if not overwrite else 'overwritten'}: {str(changedFile.name)} {'to' if not overwrite else 'in'} '{self.storage_type}':  {self.storageService.destination}" if not(failed) else f"## Failed copying: {str(changedFile.name)}")
        logging.info(f"$$ Successfully {'copied' if not overwrite else 'overwritten'}: {str(changedFile.name)} {'to' if not overwrite else 'in'} '{self.storage_type}':  {self.storageService.destination}" if not(failed) else f"## Failed overwriting: {str(changedFile.name)}")

    def is_interested(self, path: Path, recursive: bool = False):
        if self.exclude.match(str(path)):
            return False

        # a path or file in watched paths
        if path in self.paths or (path.is_file() and path.parent in self.paths):
            return True

        if recursive:
            while path.parent != path:
                # walk up towards path's root until we reach root
                path = path.parent
                if self.is_interested(path, recursive=False):
                    return True

        return False

    def on_change(self, path, overwrite=False):
        path = Path(path)
        if self.is_interested(path, recursive=self.recursive):
            # print("interesting file")
            # print("\nis file: ", path.is_file(), '\nsuffix: ', path.suffix, '\nin filetypes: ', path.suffix in self.filetypes)
            if path.is_file() and (path.suffix in self.filetypes or '*' in self.filetypes):
                self.execAction(path, overwrite)

    def on_created(self, event):
        # if self.observer.__class__.__name__ == 'InotifyObserver':
        #     # inotify also generates modified events for created files
        #     return

        if event.is_directory:
            logging.info(f'created dir {event.src_path}')
            self.on_change(event.src_path)
        else:
            self.on_change(event.src_path)

    def on_modified(self, event):
        if not event.is_directory:
            time.sleep(PAUSEAFTERMODIFIED)
            self.on_change(event.src_path, overwrite=True)

    def on_moved(self, event):
        if not event.is_directory:
            pass
            # print(event.src_path, ' file_moved')
            # self.on_change(event.dest_path)

    def on_deleted(self, event):
        if not event.is_directory:
            pass
            # print(event.src_path, ' file_deleted')
            # self.on_change(event.src_path)

    def run(self):    
        self.observer.start()
        try:
            while True:
                time.sleep(self.refreshFrequency)
        except KeyboardInterrupt:
            self.observer.stop()
        self.observer.join()


def main(args, storageService):
    filetypes = ['.'+f if not f.startswith('.') else f for f in args.filetypes.split(';')]
    paths = {Path(p).resolve():p for p in args.paths}

    # create watchdog service
    zs = Zuschauer(paths=paths, filetypes=filetypes, storage_type=args.storage_type, storageService=storageService,
        recursive=args.recursive, refreshFrequency=args.refresh, verboseMode=args.verbose, dryRun=args.dryrun)

    # upload already available files
    if args.existing:
        if args.verbose:
            print(f"""-----------------\nUpload {'recursively' if args.recursive else ''} already existing files in:
                Paths: {list(paths.keys())}, with \nFiletypes: {filetypes}, to \nStorage: {args.storage_type}
            """)
        existing_files = {}
        nExist = 0
        for path in paths.keys():
            for ft in filetypes:
                foundExistingFiles = list(path.glob(f"{'*' if args.recursive else ''}*/*"+ft))
                nExistingFiles = len(foundExistingFiles)
                if nExistingFiles:
                    existing_files[path] = foundExistingFiles
        if len(existing_files):
            if args.verbose or args.dryrun:
                print(f"Dryrun: Could have uploaded a total of {len(existing_files)} existing files.")
            if not args.dryrun:
                logging.info(f"Uploading a total of {len(existing_files)} existing files.")
                for existingFiles in existing_files.values():
                    for file_ in existingFiles:
                        if file_.is_file():
                            # upload with non-overwriting flag set to boost upload
                            zs.execAction(file_, overwrite=False)
                            if args.pipeline:
                                time.sleep(20)
        else:
            print(">>>> No existing files found. Nothing uploaded.\n-----------------\n\n")
    try:
        if args.verbose:
            print(f"""Starting watchdog with config:
                \nPaths: {list(paths.keys())}, \nFiletypes: {filetypes}, \nStorage: {args.storage_type}, \nRefreshRate: {args.refresh}
            """)
            print(f"Watch {'recursively' if args.recursive else ''} {list(paths.keys())}, action on file change\n\t{'would (--dryrun aktiv)' if args.dryrun else 'will'} copy on / overwrite in `{args.storage_type}`: {storageService.destination}.")
        # start watchdog service 
        # watch filesystem for file creation
        zs.run()
    except KeyboardInterrupt:
        print('^C')
        exit(0)


if __name__ == "__main__":
    # create Tempfile
    logging.basicConfig(filename=Path(tempfile.gettempdir()).joinpath("zuschauer.log"), level=logging.INFO,
                        format='%(asctime)s-%(levelname)s: %(name)s "%(message)s"')

    logging.info(f"Starte Zuschauer\tby {__author__}\tv.{__version__}")
    # headless arg parsing
    parser = _parse_arguments()
    _args = parser.parse_args()
    if _args.reset:
        # delete all passwords
        for key in ['account_name', 'account_key', 'cred', 'client_id', 'client_secret']:
            try:
                keyid = f"zs_{key}_{platform.node()}"
                keyval = keyring.get_password("zuschauer@drahnreb", keyid)
                keyring.delete_password("zuschauer@drahnreb", keyid)
                print(f"Deleleted key {keyid} with value {keyval}")
            except keyring.errors.PasswordDeleteError:
                pass
        # remove config file
        os.remove(CONFIGFILE)

    configFile = _args.load

    configItems = {}
    creds_available = False
    # check for credentials
    if _args.storage_type == "onPrem" or _args.storage_type == "MQTT":
        # not required, assume file share is already mounted by system
        creds_available = True
    else:
        # for cloud storage with any auth method at least account_name and account_key (resp. tenant_id) is required 
        # any creds provided by arg?
        account_name = _args.account_name
        account_key = _args.account_key
        try:
            client_id = _args.client_id
            client_secret = _args.client_secret
        except AttributeError:
            # optional args, no service principal
            client_id = client_secret = ''

        if not account_name and not account_key and STORECREDENTIALS:
            # keyring available on platform and creds saved?
            account_name = keyring.get_password("zuschauer@drahnreb", f"zs_account_name_{platform.node()}")
            account_key = keyring.get_password("zuschauer@drahnreb", f"zs_account_key_{platform.node()}")
            # retrieve all
            client_id = keyring.get_password("zuschauer@drahnreb", f"zs_client_id_{platform.node()}")
            client_secret = keyring.get_password("zuschauer@drahnreb", f"zs_client_secret_{platform.node()}")
            if account_name and account_key:
                logging.info("retrieved creds")
        
        if account_name and account_key:
            configItems["account_name"] = account_name
            configItems["account_key"] = account_key
            # store all, even empty
            configItems["client_id"] = client_id
            configItems["client_secret"] = client_secret
            creds_available = True


    if configFile.exists() and configFile.is_file():
        # config file available
        logging.info(f'Loading config from file {configFile}')
        with open(configFile, 'rt') as f:
            print(configItems)
            configItems.update(json.load(f))
            print(configItems)
        if creds_available:
            t_args = argparse.Namespace()
            try:
                # add config options that are not necessary to be specified in config file but need to be initialized
                for k in ["save", "existing", "pipeline", "dryrun", "reset"]:
                    if k not in configItems.keys():
                        configItems[k] = False
                # consume current flags
                for k, v in _args.__dict__.items():
                    if v is not None and 'load' not in k and k not in configItems.keys():  # and v != ''
                        configItems[k] = v
                t_args.__dict__.update(configItems)
                args = parser.parse_args(namespace=t_args)
            except BaseException as e:
                logging.error("Loading from config failed.", e)
                # if loading fails, prepare gooey interface
                args = parse_arguments(configItems)
        else:
            # load gooey with config data
            args = parse_arguments(configItems)
    else:
        # ask for config, prepare gooey interface
        args = parse_arguments(configItems)

    # init logger before we start connection checks
    if args.verbose:
        level = logging.INFO
    else:
        level = logging.WARNING

    # import necessary packages
    if args.storage_type == "MQTT":
        import hashlib  # chunked messages
        import paho.mqtt.client as mqtt
        logger = logging.getLogger('mqtt')
        logger.setLevel(level)

    elif args.storage_type != "onPrem":
        # azure based
        from azure.storage.blob._shared.base_client import create_configuration
        from azure.storage.blob import BlobServiceClient
        from azure.identity import ClientSecretCredential
        from azure.storage.filedatalake import DataLakeServiceClient
        from azure.core.pipeline.policies import ProxyPolicy

        azureLogger = logging.getLogger('azure')
        azureLogger.setLevel(level)
        # # Configure a console output
        # handler = logging.StreamHandler(stream=sys.stdout)
        # handler.setLevel(level)
        # azureLogger.addHandler(handler)

        if not args.ssl_verify:
            import requests
            requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

    # check args including storage client and set up storageService
    storageService = checkArgs(args)

    # persist config for restart
    if args.save:
        if args.account_name and args.account_key and STORECREDENTIALS:
            keyring.set_password("zuschauer@drahnreb", f"zs_account_name_{platform.node()}", str(args.account_name))
            keyring.set_password("zuschauer@drahnreb", f"zs_account_key_{platform.node()}", str(args.account_key))
            # store all
            keyring.set_password("zuschauer@drahnreb", f"zs_client_id_{platform.node()}", str(args.client_id))
            keyring.set_password("zuschauer@drahnreb", f"zs_client_secret_{platform.node()}", str(args.client_secret))
        config = vars(args).copy()
        config['paths'] = [str(p) for p in args.paths]
        config['dryrun'] = False
        [config.pop(k, None) for k in\
            ['save', 'reset', 'load', 'existing', 'account_name', 'account_key', 'client_id', 'client_secret']]
        with open(CONFIGFILE, 'w') as outfile:
            json.dump(config, outfile, indent=2)
    # run main
    main(args, storageService)

    # TODO:
    # shutdown routine / intercept KeyBoardInterrupt
    # disconnect MQTT...
    # client.disconnect() #disconnect
    # client.loop_stop() #stop loop
