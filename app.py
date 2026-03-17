"""Run the Arpe.io MCP server with SSE transport."""
import uvicorn
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Mount, Route
from mcp.server.sse import SseServerTransport
from src.server import app, all_tools, tool_handlers

sse = SseServerTransport("/messages/")

# Must be a raw ASGI callable (scope, receive, send), not a Starlette
# request handler. connect_sse writes directly to send and returns None,
# which Starlette would try to call as a Response, causing TypeError.
async def handle_sse(scope, receive, send):
    async with sse.connect_sse(scope, receive, send) as streams:
        await app.run(
            streams[0], streams[1], app.create_initialization_options()
        )

async def health(request):
    return JSONResponse({"status": "ok"})

async def debug(request):
    return JSONResponse({
        "tools_count": len(all_tools),
        "tool_names": [t.name for t in all_tools],
        "handlers_count": len(tool_handlers),
    })

starlette_app = Starlette(
    routes=[
        Route("/", endpoint=health),
        Route("/debug", endpoint=debug),
        Route("/sse", endpoint=handle_sse),
        Mount("/messages/", app=sse.handle_post_message),
    ],
)

if __name__ == "__main__":
    uvicorn.run(starlette_app, host="0.0.0.0", port=7860)
