import asyncio
import logging
import sys
from dotenv import load_dotenv

load_dotenv()

from google import genai
from google.genai import types as genai_types

logging.getLogger("google_genai").setLevel(logging.ERROR)


async def main(script_input: str):

    file_name = (
        script_input if script_input else "assets/LOREAL_Rapport_Annuel_2024.pdf"
    )
    doc_data = None
    with open(file_name, "rb") as f:
        doc_data = f.read()

    genai_client = genai.Client()

    prompt = """Convert this document in markdown."""
    print("Converting pdf document to markdown...")
    parsed = genai_client.models.generate_content(
        model="gemini-3-flash-preview",
        contents=[
            genai_types.Part.from_bytes(
                data=doc_data,
                mime_type="application/pdf",
            ),
            prompt,
        ],
        config=genai_types.GenerateContentConfig(
            thinking_config=genai_types.ThinkingConfig(thinking_budget=128)
        ),
    )
    if parsed.text:
        with open(f"{file_name.rsplit('.', 1)[0]}.md", "w") as f:
            f.write(parsed.text)
        print(f"Converted document saved to {file_name.rsplit('.', 1)[0]}.md")
    else:
        print("Failed to convert the document.")


if __name__ == "__main__":
    script_input = sys.argv[1] if len(sys.argv) > 1 else "Money KPIs"
    asyncio.run(main(script_input))
