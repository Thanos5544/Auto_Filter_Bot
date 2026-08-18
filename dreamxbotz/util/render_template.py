import jinja2
import urllib.parse
import logging

from info import BIN_CHANNEL, URL
from dreamxbotz.Bot import dreamxbotz
from dreamxbotz.util.human_readable import humanbytes
from dreamxbotz.util.file_properties import get_file_ids
from dreamxbotz.server.exceptions import InvalidHash


async def render_page(id, secure_hash, src=None):
    file_data = await get_file_ids(
        dreamxbotz,
        int(BIN_CHANNEL),
        int(id)
    )

    if file_data.unique_id[:6] != secure_hash:
        logging.debug(
            f"Invalid hash for message ID {id}: "
            f"{secure_hash} != {file_data.unique_id[:6]}"
        )
        raise InvalidHash

    file_name = file_data.file_name or f"file_{id}"

    # FileStream-style: player ka source direct /dl endpoint hoga
    encoded_name = urllib.parse.quote(file_name, safe="")
    file_url = urllib.parse.urljoin(
        URL,
        f"dl/{id}/{encoded_name}?hash={secure_hash}"
    )

    mime_type = file_data.mime_type or "application/octet-stream"
    file_size = humanbytes(file_data.file_size)

    media_type = mime_type.split("/", 1)[0].lower()

    if media_type in ("video", "audio"):
        template_file = "dreamxbotz/template/req.html"
    else:
        template_file = "dreamxbotz/template/dl.html"

    with open(template_file, "r", encoding="utf-8") as f:
        template = jinja2.Template(f.read())

    return template.render(
        file_name=file_name.replace("_", " "),
        file_url=file_url,
        file_size=file_size,
        file_unique_id=file_data.unique_id,
        mime_type=mime_type,
    )
