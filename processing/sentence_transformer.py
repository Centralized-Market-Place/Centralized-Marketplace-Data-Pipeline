from sentence_transformers import SentenceTransformer


SENTENCE_TRANSFORMER_MODEL_NAME = "all-MiniLM-L6-v2"
transformer_model = SentenceTransformer(SENTENCE_TRANSFORMER_MODEL_NAME)

def transform(text: str):
    embedding = transformer_model.encode(text).tolist()
    return embedding