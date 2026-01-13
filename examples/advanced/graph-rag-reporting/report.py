from dotenv import load_dotenv

load_dotenv()
import logging

logging.getLogger("google_genai").setLevel(logging.ERROR)
import sys
from simple_graph_retriever.client import GraphRetrievalClient
from simple_graph_retriever.models import RetrievalConfig
from google import genai
from google.genai import types as genai_types
from utils import wait_for_embedder, wait_for_neo4j


def main(script_input: str):
    wait_for_neo4j()
    wait_for_embedder()
    genai_client = genai.Client()
    retrieval_client = GraphRetrievalClient()
    data = retrieval_client.retrieve_graph(
        query=script_input,
        config=RetrievalConfig(
            community_score_drop_off_pct=0.3, chunk_score_drop_off_pct=0.3
        ),
    )
    if not data:
        print("No data found.")
        return

    print(
        "Nodes found:",
        len(data.nodes),
    )
    print(
        "Relationships found:",
        len(data.relationships),
    )
    report_prompt = f"""
        Provide a report summarizing the following information about {script_input}:
        {data.to_markdown()}

        The report should include key details and insights derived from the data.
        Be concise and informative.
        Only mention informations that are related to {script_input} directly or indirectly.
        Use tables when relevant to present data clearly.
    """

    report = genai_client.models.generate_content(
        model="gemini-3-flash-preview",
        contents=report_prompt,
        config=genai_types.GenerateContentConfig(
            thinking_config=genai_types.ThinkingConfig(thinking_budget=128)
        ),
    )
    if report.text:
        report_path = f"./output/report_{script_input.lower().replace(' ', '_')}.md"
        with open(report_path, "w") as f:
            f.write(f"# Report on {script_input}\n\n")
            f.write(report.text)
        print(f"Report generated and saved successfully at {report_path}")
    else:
        print("No report generated because the response was empty.")


if __name__ == "__main__":
    script_input = sys.argv[1] if len(sys.argv) > 1 else "Money KPIs"
    main(script_input)
