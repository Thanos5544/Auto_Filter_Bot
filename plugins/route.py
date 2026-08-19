from aiohttp import web
import re
import math
import logging
import mimetypes

from aiohttp.http_exceptions import BadStatusLine

from dreamxbotz.Bot import multi_clients, work_loads
from dreamxbotz.server.exceptions import FIleNotFound, InvalidHash
from dreamxbotz.util.custom_dl import ByteStreamer
from dreamxbotz.util.render_template import render_page
import info


routes = web.RouteTableDef()


@routes.get("/favicon.ico")
async def favicon_route_handler(request):
    return web.FileResponse("dreamxbotz/template/favicon.ico")


@routes.get("/", allow_head=True)
async def root_route_handler(request):
    return web.json_response("dreamxbotz")


def parse_path(path: str, request: web.Request):
    match = re.search(r"^([A-Za-z0-9_-]{6})(\d+)", path)

    if match:
        secure_hash = match.group(1)
        file_id = int(match.group(2))
        return file_id, secure_hash

    id_match = re.search(r"(\d+)", path)
    if not id_match:
        raise web.HTTPNotFound(text="Invalid path")

    file_id = int(id_match.group(1))
    secure_hash = request.rel_url.query.get("hash")

    if not secure_hash:
        raise web.HTTPForbidden(text="Missing hash")

    return file_id, secure_hash


@routes.get(r"/watch/{path:\S+}", allow_head=True)
async def watch_handler(request: web.Request):
    try:
        path = request.match_info["path"]
        file_id, secure_hash = parse_path(path, request)

        return web.Response(
            text=await render_page(file_id, secure_hash),
            content_type="text/html",
        )

    except InvalidHash as e:
        raise web.HTTPForbidden(text=str(e))
    except FIleNotFound as e:
        raise web.HTTPNotFound(text=str(e))
    except web.HTTPNotFound:
        raise
    except (AttributeError, BadStatusLine, ConnectionResetError):
        raise
    except Exception as e:
        logging.exception("Watch route error")
        raise web.HTTPInternalServerError(text=str(e))


@routes.get(r"/dl/{path:\S+}", allow_head=True)
async def player_stream_handler(request: web.Request):
    try:
        path = request.match_info["path"]
        file_id, secure_hash = parse_path(path, request)

        return await media_streamer(
            request, file_id, secure_hash, download=False
        )

    except InvalidHash as e:
        raise web.HTTPForbidden(text=str(e))
    except FIleNotFound as e:
        raise web.HTTPNotFound(text=str(e))
    except web.HTTPNotFound:
        raise
    except (AttributeError, BadStatusLine, ConnectionResetError):
        raise
    except Exception as e:
        logging.exception("Player stream route error")
        raise web.HTTPInternalServerError(text=str(e))


@routes.get(r"/download/{path:\S+}", allow_head=True)
async def download_handler(request: web.Request):
    try:
        path = request.match_info["path"]
        file_id, secure_hash = parse_path(path, request)

        return await media_streamer(
            request, file_id, secure_hash, download=True
        )

    except InvalidHash as e:
        raise web.HTTPForbidden(text=str(e))
    except FIleNotFound as e:
        raise web.HTTPNotFound(text=str(e))
    except web.HTTPNotFound:
        raise
    except (AttributeError, BadStatusLine, ConnectionResetError):
        raise
    except Exception as e:
        logging.exception("Download route error")
        raise web.HTTPInternalServerError(text=str(e))


class_cache = {}


async def media_streamer(
    request: web.Request,
    id: int,
    secure_hash: str,
    download: bool = False,
):
    range_header = request.headers.get("Range")

    index = min(work_loads, key=work_loads.get)
    faster_client = multi_clients[index]

    if getattr(info, "MULTI_CLIENT", False):
        logging.info(
            "Client %s serving %s",
            index,
            request.headers.get("X-FORWARDED-FOR", request.remote),
        )

    if faster_client not in class_cache:
        class_cache[faster_client] = ByteStreamer(faster_client)

    tg_connect = class_cache[faster_client]

    file_data = await tg_connect.get_file_properties(id)

    if file_data.unique_id[:6] != secure_hash:
        raise InvalidHash

    file_size = file_data.file_size

    if range_header:
        range_value = range_header.replace("bytes=", "", 1)
        start_text, end_text = range_value.split("-", 1)
        from_bytes = int(start_text or 0)
        until_bytes = int(end_text) if end_text else file_size - 1
    else:
        http_range = request.http_range
        from_bytes = http_range.start or 0
        until_bytes = (
            (http_range.stop - 1)
            if http_range.stop is not None
            else file_size - 1
        )

    if (
        from_bytes < 0
        or from_bytes >= file_size
        or until_bytes < from_bytes
        or until_bytes >= file_size
    ):
        return web.Response(
            status=416,
            text="416: Range not satisfiable",
            headers={"Content-Range": f"bytes */{file_size}"},
        )

    chunk_size = 1024 * 1024
    offset = from_bytes - (from_bytes % chunk_size)
    first_part_cut = from_bytes - offset
    last_part_cut = (until_bytes % chunk_size) + 1
    request_length = until_bytes - from_bytes + 1
    part_count = (
        math.ceil(until_bytes / chunk_size)
        - math.floor(offset / chunk_size)
    )

    body = tg_connect.yield_file(
        file_data,
        index,
        offset,
        first_part_cut,
        last_part_cut,
        part_count,
        chunk_size,
    )

    file_name = getattr(file_data, "file_name", None) or f"file_{id}"
    mime_type = getattr(file_data, "mime_type", None)

    if not mime_type:
        mime_type = (
            mimetypes.guess_type(file_name)[0]
            or "application/octet-stream"
        )

    disposition = "attachment" if download else "inline"

    return web.Response(
        status=206 if range_header else 200,
        body=body,
        headers={
            "Content-Type": mime_type,
            "Content-Range": f"bytes {from_bytes}-{until_bytes}/{file_size}",
            "Content-Length": str(request_length),
            "Content-Disposition": f'{disposition}; filename="{file_name}"',
            "Accept-Ranges": "bytes",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
            "Access-Control-Allow-Headers": "Range, Content-Type",
            "Access-Control-Expose-Headers": (
                "Content-Length, Content-Range, Accept-Ranges"
            ),
        },
                              )
