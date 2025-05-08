import ollama
import numpy as np
import time
from typing import List
import os
# Configure the client to use your custom server
# ollama.BASE_URL = os.getenv("OLLAMA_BASE_URL")

def get_embeddings_batch(client, texts: List[str], model: str = "bge-m3", batch_size: int = 10):
    """
    Get embeddings for a list of texts in batches.
    
    Args:
        texts (List[str]): List of texts to embed
        model (str): Model name to use for embeddings
        batch_size (int): Number of texts to process in each batch
        
    Returns:
        List[List[float]]: List of embedding vectors
    """
    all_embeddings = []
    
    # Process in batches
    for i in range(0, len(texts), batch_size):
        batch = texts[i:i + batch_size]
        print(f"Processing batch {i // batch_size + 1}/{(len(texts) + batch_size - 1) // batch_size} "
              f"({len(batch)} texts)")
        
        try:
            # Process each text in the batch individually
            response=client.embed(model=model, input=batch)
            all_embeddings.extend(response["embeddings"])
            print(f"Completed batch {i // batch_size + 1}")
            
            # Add a small delay between batches to avoid overwhelming the server
            if i + batch_size < len(texts):
                time.sleep(0.5)
                
        except Exception as e:
            print(f"Error processing batch {i // batch_size + 1}: {e}")
            # Continue with the next batch instead of failing completely
            continue
    
    return all_embeddings

# Example usage
if __name__ == "__main__":
    client = ollama.Client(
        host=os.getenv("OLLAMA_BASE_URL"),
    )
    # Create a sample list of texts
    sample_texts = [
        "The quick brown fox jumps over the lazy dog.",
        "Machine learning is a field of artificial intelligence.",
        "Natural language processing helps computers understand human language.",
        "Vector embeddings represent text as numerical vectors.",
        "Python is a popular programming language for data science.",
        "The sky is blue on a clear day.",
        "Neural networks are inspired by the human brain.",
        "Data visualization helps to understand complex patterns.",
        "Clustering is an unsupervised learning technique.",
        "Semantic search uses meaning rather than keywords.",
        "Transformers have revolutionized natural language processing.",
        "Language models can generate human-like text.",
        "The Eiffel Tower is located in Paris, France.",
        "Coffee contains caffeine which is a stimulant.",
        "Quantum computers use quantum bits or qubits."
    ]
    
    # Process with a batch size of 5
    print(f"Processing {len(sample_texts)} texts with batch size 5")
    start_time = time.time()
    embeddings = get_embeddings_batch(client, sample_texts, batch_size=5)
    end_time = time.time()
    
    # Display results
    print(f"\nProcessing completed in {end_time - start_time:.2f} seconds")
    print(f"Generated {len(embeddings)} embeddings")
    
    if embeddings:
        # Show embedding dimensions
        embedding_dim = len(embeddings[0])
        print(f"Embedding dimension: {embedding_dim}")
        
        # Calculate and print the average embedding norm
        norms = [np.linalg.norm(emb) for emb in embeddings]
        avg_norm = sum(norms) / len(norms)
        print(f"Average embedding L2 norm: {avg_norm:.4f}")
        
        # Calculate cosine similarity between first and second text
        if len(embeddings) >= 2:
            def cosine_similarity(a, b):
                return np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b))
            
            sim = cosine_similarity(embeddings[0], embeddings[1])
            print(f"\nSample similarity between first two texts: {sim:.4f}")
            
            # Show most similar pair
            max_sim = 0
            max_pair = (0, 0)
            for i in range(len(embeddings)):
                for j in range(i+1, len(embeddings)):
                    sim = cosine_similarity(embeddings[i], embeddings[j])
                    if sim > max_sim:
                        max_sim = sim
                        max_pair = (i, j)
            
            print(f"Most similar pair:")
            print(f"- Text 1: \"{sample_texts[max_pair[0]]}\"")
            print(f"- Text 2: \"{sample_texts[max_pair[1]]}\"")
            print(f"- Similarity: {max_sim:.4f}")