import json
import time
import requests
import re
from langgraph.graph import StateGraph, END
from typing import TypedDict

# Constants


API_KEY = "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" # Gelo
API_KEY2 = "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" # Million
MODEL = "meta-llama/llama-4-scout-17b-16e-instruct"
API_URL = "https://api.groq.com/openai/v1/chat/completions"
OUTPUT_FILE = "structured_output.json"

API_KEY2
# Utility: Append to JSON file
# def append_to_output(data):
#     try:
#         with open(OUTPUT_FILE, "r+", encoding="utf-8") as f:
#             existing = json.load(f)
#             existing.append(data)
#             f.seek(0)
#             json.dump(existing, f, ensure_ascii=False, indent=2)
#     except FileNotFoundError:
#         with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
#             json.dump([data], f, ensure_ascii=False, indent=2)

# LangGraph workflow
class GraphState(TypedDict, total=False):
    post: str
    result: dict
    decision: str

def is_product_tool(input_text: str) -> bool:
    try:
        prompt = f"Does the following text describe a product being sold? Answer 'yes' or 'no'.\n\nPost:\n{input_text}"
        response = requests.post(
            API_URL,
            headers={"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"},
            json={"model": MODEL, "messages": [{"role": "user", "content": prompt}]}
        )
        response.raise_for_status()
        content = response.json()["choices"][0]["message"]["content"].lower()
        is_product = "yes" in content
        print(f"LLM Product Decision: '{content}', Is Product: {is_product}")
        return is_product
    except Exception as e:
        error_data = {"error": f"Product check failed: {str(e)}", "original": input_text}
        # append_to_output(error_data)
        return False

def extract_entities(description: str):
    try:
        prompt = f"""
Extract structured information from the following post. Return a JSON object with:
- title
- price
- category: one of ["technology", "clothes", "shoes", "accessories"]
- location
- phone
- link

Respond with valid JSON only. No explanation.

Post:
\"\"\"{description}\"\"\""""
        response = requests.post(
            API_URL,
            headers={"Authorization": f"Bearer {API_KEY2}", "Content-Type": "application/json"},  # 🔄 Uses API_KEY2 now
            json={"model": MODEL, "messages": [{"role": "user", "content": prompt}]}
        )
        response.raise_for_status()
        raw = response.json()["choices"][0]["message"]["content"]
        match = re.search(r"\{.*\}", raw, re.DOTALL)
        if match:
            extracted_data = json.loads(match.group(0))
            # print(f"Extracted Data: {extracted_data}")
            print(f"✅ Extracted")
            # append_to_output({"extracted": extracted_data})
            return {"extracted": extracted_data}
        else:
            error_data = {"error": "No JSON found in response", "raw_response": raw}
            # append_to_output(error_data)
            return error_data
    except Exception as e:
        error_data = {"error": f"Extraction error: {str(e)}", "raw_response": raw if 'raw' in locals() else ""}
        # append_to_output(error_data)
        return error_data
def decide_node(state: GraphState) -> dict:
    is_product = is_product_tool(state["post"])
    decision = "extract" if is_product else "skip"
    print(f"Decision Node Output: {decision}")
    return {"decision": decision}

def extract_node(state: GraphState):
    print("Entering extract_node.")
    return extract_entities(state["post"])

def skip_node(state: GraphState):
    print("Entering skip_node.")
    skipped_data = {"skipped": True, "original": state["post"]}
    # append_to_output(skipped_data)
    return skipped_data

# Build LangGraph
graph = StateGraph(GraphState)
graph.add_node("decide", decide_node)
graph.add_node("extract", extract_node)
graph.add_node("skip", skip_node)
graph.add_conditional_edges("decide", lambda state: state["decision"], {"extract": "extract", "skip": "skip"})
graph.add_edge("extract", END)
graph.add_edge("skip", END)
graph.set_entry_point("decide")
app = graph.compile()

# Function to process a single text input
def process_description(input_text: str):
    if not input_text.strip():
        print("Error: Empty description")
        return 
    print(f"\n--- Processing Input ---")
    result = app.invoke({"post": input_text.strip()})
    # print(f"✅ Final Result: {result}")
    return result

# Example usage
# if __name__ == "__main__":
#     user_input = input("Enter a product description:\n")
#     process_description(user_input)
