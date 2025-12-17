import re
def rich_text(text: str):
    return [{"type": "text", "text": {"content": text}}]
def markdown_to_notion_blocks(md: str):
    """
    Convert markdown text into Notion API blocks (children)
    """
    blocks = []
    lines = md.splitlines()

    in_code_block = False
    code_buffer = []
    code_language = "plain"

    for raw_line in lines:
        line = raw_line.rstrip()

        # ---- code block handling ----
        if line.startswith("```"):
            if not in_code_block:
                in_code_block = True
                code_language = line.replace("```", "").strip() or "plain"
                code_buffer = []
            else:
                blocks.append({
                    "object": "block",
                    "type": "code",
                    "code": {
                        "language": code_language,
                        "rich_text": rich_text("\n".join(code_buffer))
                    }
                })
                in_code_block = False
            continue

        if in_code_block:
            code_buffer.append(line)
            continue

        if not line.strip():
            continue

        # ---- headings ----
        if line.startswith("### "):
            blocks.append(_heading(line[4:], 3))
        elif line.startswith("## "):
            blocks.append(_heading(line[3:], 2))
        elif line.startswith("# "):
            blocks.append(_heading(line[2:], 1))

        # ---- blockquote ----
        elif line.startswith("> "):
            blocks.append({
                "object": "block",
                "type": "quote",
                "quote": {
                    "rich_text": rich_text(line[2:])
                }
            })

        # ---- numbered list ----
        elif re.match(r"\d+\.\s+", line):
            blocks.append({
                "object": "block",
                "type": "numbered_list_item",
                "numbered_list_item": {
                    "rich_text": rich_text(re.sub(r"\d+\.\s+", "", line))
                }
            })

        # ---- bullet list ----
        elif line.startswith("- "):
            blocks.append({
                "object": "block",
                "type": "bulleted_list_item",
                "bulleted_list_item": {
                    "rich_text": rich_text(line[2:])
                }
            })

        # ---- paragraph ----
        else:
            blocks.append({
                "object": "block",
                "type": "paragraph",
                "paragraph": {
                    "rich_text": rich_text(line)
                }
            })

    return blocks


def _heading(text: str, level: int):
    level = min(level, 3)
    return {
        "object": "block",
        "type": f"heading_{level}",
        f"heading_{level}": {
            "rich_text": rich_text(text)
        }
    }
