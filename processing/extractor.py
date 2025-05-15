import json
import time
import requests
import re
from langgraph.graph import StateGraph, END
from typing import TypedDict

# === API Constants ===
API_KEY = "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"      # Gelo
API_KEY2 = "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"     # Million
API_KEY3 = "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"     # GELO_2

MODEL = "meta-llama/llama-4-scout-17b-16e-instruct"
API_URL = "https://api.groq.com/openai/v1/chat/completions"

# === LangGraph State ===
class GraphState(TypedDict, total=False):
    post: str
    result: dict
    decision: str

# === Step 1: Product Check ===
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
        print(f"❌ Product check failed: {e}")
        return False

# === Step 2: Extract Entities ===
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
            headers={"Authorization": f"Bearer {API_KEY2}", "Content-Type": "application/json"},
            json={"model": MODEL, "messages": [{"role": "user", "content": prompt}]}
        )
        response.raise_for_status()
        raw = response.json()["choices"][0]["message"]["content"]
        match = re.search(r"\{.*\}", raw, re.DOTALL)
        if match:
            extracted_data = json.loads(match.group(0))
            print(f"✅ Extracted")
            return {"extracted": extracted_data}
        else:
            print("⚠️ No JSON object found.")
            return {"error": "No JSON found in response", "raw_response": raw}
    except Exception as e:
        print(f"❌ Extraction error: {e}")
        return {"error": f"Extraction error: {str(e)}", "raw_response": raw if 'raw' in locals() else ""}

# === Step 3: Extract Hierarchical Categories ===
def extract_categories(description: str):
    try:
        prompt = f"""
From the following product description, extract a hierarchical list of categories it belongs to.
Respond only with a JSON list of strings, from general to specific.

Example: ["technology", "computer", "laptop"]

Description:
\"\"\"{description}\"\"\""""
        response = requests.post(
            API_URL,
            headers={"Authorization": f"Bearer {API_KEY3}", "Content-Type": "application/json"},
            json={"model": MODEL, "messages": [{"role": "user", "content": prompt}]}
        )
        response.raise_for_status()
        raw = response.json()["choices"][0]["message"]["content"]
        match = re.search(r"\[.*\]", raw, re.DOTALL)
        if match:
            category_list = json.loads(match.group(0))
            print(f"📂 Categories: {category_list}")
            return category_list
        else:
            print("⚠️ No valid JSON list found for categories.")
            return []
    except Exception as e:
        print(f"❌ Category extraction error: {e}")
        return []

# === Graph Nodes ===
def decide_node(state: GraphState) -> dict:
    is_product = is_product_tool(state["post"])
    decision = "extract" if is_product else "skip"
    print(f"Decision Node Output: {decision}")
    return {"decision": decision}

def extract_node(state: GraphState):
    print("Entering extract_node.")
    extraction = extract_entities(state["post"])
    return {"result": extraction}

def categorize_node(state: GraphState):
    print("Entering categorize_node.")
    description = state["post"]
    categories = extract_categories(description)
    result = state["result"]
    if result and "extracted" in result:
        result["extracted"]["categories"] = categories
    return {"result": result}

def skip_node(state: GraphState):
    print("Entering skip_node.")
    skipped_data = {"skipped": True, "original": state["post"]}
    return skipped_data

# === Build Graph ===
graph = StateGraph(GraphState)
graph.add_node("decide", decide_node)
graph.add_node("extract", extract_node)
graph.add_node("categorize", categorize_node)
graph.add_node("skip", skip_node)

graph.add_conditional_edges("decide", lambda state: state["decision"], {"extract": "extract", "skip": "skip"})
graph.add_edge("extract", "categorize")
graph.add_edge("categorize", END)
graph.add_edge("skip", END)

graph.set_entry_point("decide")
app = graph.compile()

# === Run Single Description ===
def process_description(input_text: str):
    if not input_text.strip():
        print("❌ Error: Empty description")
        return 
    print(f"\n--- Processing Input ---")
    result = app.invoke({"post": input_text.strip()})
    print('=================')
    if result.get("decision") == "extract":
        print(f"Extracted Data:\n{json.dumps(result.get('result'), indent=2)}")
        return result.get("result")
    else:
        print("Post was skipped (not a product).")
    return None

# === Example Usage ===
# if __name__ == "__main__":
#     user_input = input("Enter a product description:\n")
#     process_description(user_input)
