"""Support for functionality to download files."""
from http import HTTPStatus
import logging
import os
import re
import threading

import requests
import voluptuous as vol

from homeassistant.core import HomeAssistant, ServiceCall
import homeassistant.helpers.config_validation as cv
from homeassistant.helpers.typing import ConfigType
from homeassistant.util import raise_if_invalid_filename, raise_if_invalid_path

_LOGGER = logging.getLogger(__name__)

ATTR_FILENAME = "filename"
ATTR_SUBDIR = "subdir"
ATTR_URL = "url"
ATTR_OVERWRITE = "overwrite"

CONF_DOWNLOAD_DIR = "download_dir"

DOMAIN = "downloader"
DOWNLOAD_FAILED_EVENT = "download_failed"
DOWNLOAD_COMPLETED_EVENT = "download_completed"

SERVICE_DOWNLOAD_FILE = "download_file"

SERVICE_DOWNLOAD_FILE_SCHEMA = vol.Schema(
    {
        vol.Required(ATTR_URL): cv.url,
        vol.Optional(ATTR_SUBDIR): cv.string,
        vol.Optional(ATTR_FILENAME): cv.string,
        vol.Optional(ATTR_OVERWRITE, default=False): cv.boolean,
    }
)

CONFIG_SCHEMA = vol.Schema(
    {DOMAIN: vol.Schema({vol.Required(CONF_DOWNLOAD_DIR): cv.string})},
    extra=vol.ALLOW_EXTRA,
)

def setup(hass: HomeAssistant, config: ConfigType) -> bool:
    """Listen for download events to download files."""
    download_path = config[DOMAIN][CONF_DOWNLOAD_DIR]
    if not os.path.isabs(download_path):
        download_path = hass.config.path(download_path)
    if not os.path.isdir(download_path):
        _LOGGER.error(
            "Download path %s does not exist. File Downloader not active", download_path
        )
        return False

    def download_file(service: ServiceCall) -> None:
        """Start thread to download the file specified in the URL."""
        url = service.data[ATTR_URL]
        subdir = service.data.get(ATTR_SUBDIR)
        filename = service.data.get(ATTR_FILENAME)
        overwrite = service.data.get(ATTR_OVERWRITE)
        threading.Thread(target=do_download, args=(url, subdir, filename, overwrite)).start()

    hass.services.register(
        DOMAIN,
        SERVICE_DOWNLOAD_FILE,
        download_file,
        schema=SERVICE_DOWNLOAD_FILE_SCHEMA,
    )
    return True

def do_download(url, subdir, filename, overwrite):
    """Download the file."""
    try:
        subdir = subdir or ""
        raise_if_invalid_path(subdir)

        filename = resolve_filename(url, filename)
        raise_if_invalid_filename(filename)

        final_path = create_final_path(download_path, subdir, filename, overwrite)

        _LOGGER.debug("%s -> %s", url, final_path)
        download_and_save_file(url, final_path)

        _LOGGER.debug("Downloading of %s done", url)
        hass.bus.fire(
            f"{DOMAIN}_{DOWNLOAD_COMPLETED_EVENT}",
            {"url": url, "filename": filename},
        )
    except (requests.exceptions.ConnectionError, ValueError) as e:
        handle_download_failure(url, filename, final_path, e)

def resolve_filename(url, filename):
    """Resolve the filename to use for the downloaded file."""
    if not filename and "content-disposition" in req.headers:
        match = re.findall(
            r"filename=(\S+)", req.headers["content-disposition"]
        )
        if match:
            filename = match[0].strip("'\" ")
    if not filename:
        filename = os.path.basename(url).strip()
    return filename

def create_final_path(download_path, subdir, filename, overwrite):
    """Create the final path for the downloaded file."""
    if subdir:
        subdir_path = os.path.join(download_path, subdir)
        os.makedirs(subdir_path, exist_ok=True)
        final_path = os.path.join(subdir_path, filename)
    else:
        final_path = os.path.join(download_path, filename)

    if not overwrite:
        final_path = ensure_unique_filename(final_path)
    
    return final_path

def ensure_unique_filename(final_path):
    """Ensure the filename is unique to avoid overwriting."""
    path, ext = os.path.splitext(final_path)
    tries = 1
    while os.path.isfile(final_path):
        tries += 1
        final_path = f"{path}_{tries}.{ext}"
    return final_path

def download_and_save_file(url, final_path):
    """Download and save the file from the URL."""
    req = requests.get(url, stream=True, timeout=10)
    if req.status_code == HTTPStatus.OK:
        with open(final_path, "wb") as fil:
            for chunk in req.iter_content(1024):
                fil.write(chunk)
    else:
        _LOGGER.warning(
            "Downloading '%s' failed, status_code=%d", url, req.status_code
        )
        handle_download_failure(url, None, final_path)

def handle_download_failure(url, filename, final_path, exception=None):
    """Handle download failure by logging and firing the event."""
    if exception:
        _LOGGER.exception(f"Download failed for {url}")
    if final_path and os.path.isfile(final_path):
        os.remove(final_path)
    hass.bus.fire(
        f"{DOMAIN}_{DOWNLOAD_FAILED_EVENT}",
        {"url": url, "filename": filename},
    )
