"""Small, safe Markdown renderer for administrator-controlled notice copy."""
from __future__ import annotations

from html import escape
import re

from markupsafe import Markup


_INLINE = re.compile(
    r"(`[^`\n]+`|\*\*[^*\n]+\*\*|\[[^\]\n]+\]\(https?://[^)\s]+\)|<https?://[^>\s]+>)"
)
_LINK = re.compile(r"\[([^\]\n]+)\]\((https?://[^)\s]+)\)")


def _inline(value: str) -> str:
    parts: list[str] = []
    position = 0
    for match in _INLINE.finditer(value):
        parts.append(escape(value[position : match.start()]))
        token = match.group(0)
        if token.startswith("`"):
            parts.append(f"<code>{escape(token[1:-1])}</code>")
        elif token.startswith("**"):
            parts.append(f"<strong>{escape(token[2:-2])}</strong>")
        elif token.startswith("["):
            link = _LINK.fullmatch(token)
            assert link is not None
            parts.append(
                f'<a href="{escape(link.group(2), quote=True)}">'
                f"{escape(link.group(1))}</a>"
            )
        else:
            url = token[1:-1]
            parts.append(
                f'<a href="{escape(url, quote=True)}">{escape(url)}</a>'
            )
        position = match.end()
    parts.append(escape(value[position:]))
    return "".join(parts)


def render_notice_markdown(value: str) -> Markup:
    """Render the notice's headings, lists and inline syntax after HTML escaping."""
    lines = value.splitlines()
    blocks: list[str] = []
    index = 0
    while index < len(lines):
        line = lines[index].strip()
        if not line:
            index += 1
            continue
        heading = re.fullmatch(r"(#{1,6})\s+(.+)", line)
        if heading:
            level = len(heading.group(1))
            blocks.append(f"<h{level}>{_inline(heading.group(2))}</h{level}>")
            index += 1
            continue
        if line.startswith("- "):
            items: list[str] = []
            while index < len(lines) and lines[index].strip().startswith("- "):
                item = [lines[index].strip()[2:]]
                index += 1
                while index < len(lines):
                    continuation = lines[index].strip()
                    if (
                        not continuation
                        or continuation.startswith("- ")
                        or re.fullmatch(r"#{1,6}\s+.+", continuation)
                    ):
                        break
                    item.append(continuation)
                    index += 1
                items.append(f"<li>{_inline(' '.join(item))}</li>")
            blocks.append("<ul>" + "".join(items) + "</ul>")
            continue
        paragraph = [line]
        index += 1
        while index < len(lines):
            candidate = lines[index].strip()
            if (
                not candidate
                or candidate.startswith("- ")
                or re.fullmatch(r"#{1,6}\s+.+", candidate)
            ):
                break
            paragraph.append(candidate)
            index += 1
        blocks.append(f"<p>{_inline(' '.join(paragraph))}</p>")
    return Markup("\n".join(blocks))
