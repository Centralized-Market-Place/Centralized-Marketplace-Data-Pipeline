import json
import time
import re
from langgraph.graph import StateGraph, END
from typing import TypedDict
import threading
import os
from processing.price_cleaner import clean_price
from processing.category_validator import validate_and_clean_categories, ensure_list, ensure_string
from processing.sentence_transformer import transform
from groq import Groq


# === API Constants ===
# API_KEY = "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"      # Gelo


# Initialize Groq client
groq_client = Groq(
    api_key=os.environ.get("GROQ_API_KEY") or "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",
)

AI_MODEL = "meta-llama/llama-4-scout-17b-16e-instruct" 
MAX_CHAR_LENGTH = 2000

# Simple rate limiter: allow 1 request per second
rate_limit_lock = threading.Lock()
last_request_time = [0]
RATE_LIMIT_SECONDS = 2.0

def ask_ai(prompt):
    messages = [{"role": "user", "content": prompt}] 
    with rate_limit_lock:
        now = time.time()
        wait = last_request_time[0] + RATE_LIMIT_SECONDS - now
        if wait > 0:
            time.sleep(wait)
        last_request_time[0] = time.time()

    response = groq_client.chat.completions.create(
        model=AI_MODEL,
        messages=messages,
    )
    return response.choices[0].message.content

# def together_chat(messages):
#     with rate_limit_lock:
#         now = time.time()
#         wait = last_request_time[0] + RATE_LIMIT_SECONDS - now
#         if wait > 0:
#             time.sleep(wait)
#         last_request_time[0] = time.time()
#     return together_client.chat.completions.create(model=MODEL, messages=messages)

# === LangGraph State ===
class GraphState(TypedDict, total=False):
    post: str
    result: dict
    decision: str

# === Step 1: Product Check ===
def is_product_tool(input_text: str) -> bool:
    try:
        prompt = f"No explanation. Does the following text describe a product being sold? Answer 'yes' or 'no'.\n\nPost:\n{input_text}"
        response = ask_ai(prompt)
        content = response.lower()
        is_product = ("yes" in content) and ("no" not in content)
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
- location
- phone
- link

Respond with valid JSON only. No explanation.

Post:
\"\"\"{description}\"\"\""""
        response = ask_ai(prompt)
        raw = response
        # Use non-greedy regex to extract the first JSON object
        match = re.search(r"\{.*?\}", raw, re.DOTALL)
        if match:
            try:
                extracted_data = json.loads(match.group(0))
                print(f"✅ Extracted")
                return {"extracted": extracted_data}
            except Exception as e:
                print(f"❌ Extraction error (json.loads): {e}")
                return {"error": f"Extraction error: {str(e)}", "raw_response": match.group(0)}
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
Extract a detailed, hierarchical category list from the product description.

Use only from the following fixed categories:
Top-level: ["technology", "clothes", "shoes", "accessories"]
Subcategories:
- technology: ["phones", "smartphones", "android", "iphone", "laptops", "gaming", "tablets", "wearables"]
- clothes: ["men", "women", "dresses", "shirts", "t-shirts", "jeans", "evening dresses"]
- shoes: ["men", "women", "formal", "casual", "oxford", "sneakers", "boots"]
- accessories: ["bags", "backpacks", "laptop bags", "jewelry", "watches", "belts"]

Return a JSON list (from general to specific), 3+ levels if possible.
Do not invent new terms.
No explanations. JSON only.

Examples:
- ["technology", "phones", "smartphones", "android"]
- ["clothes", "women", "dresses", "evening dresses"]

Description:
\"\"\"{description}\"\"\"
"""
        response = ask_ai(prompt)
        raw = response
        match = re.search(r"\[.*\]", raw, re.DOTALL)
        if match:
            category_list = json.loads(match.group(0))
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
    return {"decision": decision}

def extract_node(state: GraphState):
    extraction = extract_entities(state["post"])
    return {"result": extraction}

def categorize_node(state: GraphState):
    description = state["post"]
    categories = extract_categories(description)
    result = state["result"]
    if result and "extracted" in result:
        result["extracted"]["categories"] = categories
    return {"result": result}

def skip_node(state: GraphState):
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

# === Truncate Input ===
def truncate_input(text: str) -> str:
    return text[:MAX_CHAR_LENGTH]

# === Run Single Description ===
def process_description(input_text: str):
    if not input_text.strip():
        return 
    
    input_text = truncate_input(input_text)
    result = app.invoke({"post": input_text.strip()})
    if result.get("decision") == "extract":
        return result.get("result")
    else:
        print("Post was skipped (not a product).")
    
    return None

def extract(text: str):
    try:
        result = process_description(text)    
        extracted = result.get("extracted") if isinstance(result, dict) else None
        
        if not extracted: 
            return None, None
        extracted['title'] = ensure_string(extracted.get('title', ''))
        # Skip the message if no title is extracted
        if not extracted.get("title"):
            return None, None

        try:
            extracted['price'] = clean_price(extracted.get('price'))
        except Exception as e:
            print("❌ Error trying to clean price")

        extracted['categories'] = validate_and_clean_categories(extracted.get('categories', []))
        extracted['location'] = ensure_string(extracted.get('location', ''))
        extracted['phone'] = ensure_list(extracted.get('phone', []))
        extracted['link'] = ensure_list(extracted.get('link', []))
        doc_embedding = transform(text)
        return extracted, doc_embedding
    except Exception as e:
        print(f"Error processing message: {e}")
        return None, None

# # === Example Usage ===
# if __name__ == "__main__":
#     user_input = input("Enter a product description:\n")
#     process_description(user_input)
