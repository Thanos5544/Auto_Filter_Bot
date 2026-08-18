#Thanks @dreamxbotz for helping in this journey 
import jinja2
from info import BIN_CHANNEL, URL
from dreamxbotz.Bot import dreamxbotz
from dreamxbotz.util.human_readable import humanbytes
from dreamxbotz.util.file_properties import get_file_ids
from dreamxbotz.server.exceptions import InvalidHash
import urllib.parse
import logging
import aiohttp

async def render_page(id, secure_hash, src=None):
    file = await dreamxbotz.get_messages(int(BIN_CHANNEL), int(id))
    file_data = await get_file_ids(dreamxbotz, int(BIN_CHANNEL), int(id))
    if file_data.unique_id[:6] != secure_hash:
        logging.debug(f"link hash: {secure_hash} - {file_data.unique_id[:6]}")
        logging.debug(f"Invalid hash for message with - ID {id}")
        raise InvalidHash

    file_name_quoted = urllib.parse.quote_plus(file_data.file_name)
    # stream = inline, download = attachment
    src = urllib.parse.urljoin(URL, f"{id}/{file_name_quoted}?hash={secure_hash}")
    download_src = urllib.parse.urljoin(URL, f"dl/{id}/{file_name_quoted}?hash={secure_hash}")

    tag = file_data.mime_type.split("/")[0].strip() if file_data.mime_type else ""
    file_size = humanbytes(file_data.file_size)
    if tag in ["video", "audio"]:
        template_file = "dreamxbotz/template/req.html"
    else:
        template_file = "dreamxbotz/template/dl.html"

    with open(template_file) as f:
        template = jinja2.Template(f.read())

    file_name = file_data.file_name.replace("_", " ")

    # VLC / MX ke liye sahi link - direct stream wala
    vlc_url = f"vlc://{src}"
    mx_url = f"intent:{src}#Intent;package=com.mxtech.videoplayer.ad;S.title={file_name_quoted};end"
    nplayer_url = f"intent:{src}#Intent;package=com.genuine.leone;S.title={file_name_quoted};end"

    return template.render(
        file_name=file_name,
        file_url=src,  # <video> ke liye
        download_url=download_src,  # Download button ke liye
        file_size=file_size,
        file_unique_id=file_data.unique_id,
        mime_type=file_data.mime_type,
        vlc_url=vlc_url,
        mx_url=mx_url,
        nplayer_url=nplayer_url,
    )
