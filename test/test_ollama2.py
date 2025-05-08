import requests
import numpy as np
import time
import json
import sys
from typing import List, Optional, Dict, Any

# Configure API endpoint
OLLAMA_BASE_URL = "http://10.24.116.15:8998"

def check_connection():
    """Test connection to the Ollama server"""
    try:
        response = requests.get(f"{OLLAMA_BASE_URL}/api/version", timeout=5)
        if response.status_code == 200:
            version = response.json().get("version", "unknown")
            print(f"✓ Connected to Ollama server at {OLLAMA_BASE_URL} (version: {version})")
            return True
        else:
            print(f"✗ Connection failed: HTTP {response.status_code}")
            return False
    except Exception as e:
        print(f"✗ Connection error: {e}")
        return False

def get_available_models():
    """Get list of available models on the server"""
    try:
        response = requests.get(f"{OLLAMA_BASE_URL}/api/tags", timeout=5)
        if response.status_code == 200:
            models = response.json().get("models", [])
            model_names = [m["name"] for m in models]
            print(f"✓ Found {len(model_names)} available models")
            
            # Display a few model names
            if model_names:
                sample = model_names[:5]
                print(f"  Sample models: {', '.join(sample)}")
                
                # Look for embedding models
                embed_models = [m for m in model_names if any(x in m.lower() for x in ["embed", "bge", "minilm"])]
                if embed_models:
                    print(f"  Possible embedding models: {', '.join(embed_models)}")
            
            return model_names
        else:
            print(f"✗ Failed to get model list: HTTP {response.status_code}")
            return []
    except Exception as e:
        print(f"✗ Error getting model list: {e}")
        return []

def pull_model(model_name):
    """Pull a model from Ollama repository"""
    print(f"Attempting to pull model: {model_name}")
    try:
        url = f"{OLLAMA_BASE_URL}/api/pull"
        payload = {"name": model_name}
        response = requests.post(url, json=payload, stream=True)
        
        if response.status_code == 200:
            print(f"✓ Successfully started pulling {model_name}. This may take some time.")
            
            # Since this is a streaming response, we'll show progress
            for line in response.iter_lines():
                if line:
                    try:
                        data = json.loads(line)
                        if "status" in data:
                            print(f"  {data['status']}")
                        if "completed" in data and data["completed"]:
                            print(f"✓ Pull completed!")
                            return True
                    except:
                        pass
            return True
        else:
            print(f"✗ Failed to pull model: HTTP {response.status_code}")
            return False
    except Exception as e:
        print(f"✗ Error pulling model: {e}")
        return False

def get_embeddings(texts: List[str], model: str, endpoint: str = "embeddings", batch_size: int = 1) -> Optional[List[List[float]]]:
    """
    Get embeddings for a list of texts
    
    Args:
        texts: List of strings to embed
        model: Model name to use
        endpoint: API endpoint to use (embeddings or embed)
        batch_size: Number of texts to process in each batch
        
    Returns:
        List of embedding vectors or None if failed
    """
    if not texts:
        return []
    
    all_embeddings = []
    
    # Process in batches
    for i in range(0, len(texts), batch_size):
        batch = texts[i:i + batch_size]
        
        print(f"Processing batch {i//batch_size + 1}/{(len(texts) + batch_size - 1)//batch_size} ({len(batch)} texts)")
        
        try:
            url = f"{OLLAMA_BASE_URL}/api/{endpoint}"
            
            # Prepare payload based on endpoint
            if endpoint == "embeddings":
                # For single text input
                if len(batch) == 1:
                    payload = {
                        "model": model,
                        "prompt": batch[0],
                    }
                # For multiple texts
                else:
                    payload = {
                        "model": model,
                        "prompt": batch,
                    }
            elif endpoint == "embed":
                # For single text input
                if len(batch) == 1:
                    payload = {
                        "model": model,
                        "input": batch[0],
                    }
                # For multiple texts
                else:
                    payload = {
                        "model": model,
                        "input": batch,
                    }
            
            response = requests.post(url, json=payload, timeout=60)
            response.raise_for_status()
            result = response.json()
            
            # Handle the response format
            if "embeddings" in result:
                batch_embeddings = result["embeddings"]
                all_embeddings.extend(batch_embeddings)
                print(f"✓ Successfully processed batch with {len(batch_embeddings)} embeddings")
            elif "embedding" in result:
                all_embeddings.append(result["embedding"])
                print("✓ Successfully processed single embedding")
            else:
                print(f"✗ Unexpected response format: {result.keys()}")
                return None
            
            # Add small delay between batches
            if i + batch_size < len(texts):
                time.sleep(1)
                
        except Exception as e:
            print(f"✗ Error processing batch: {e}")
            if hasattr(e, 'response') and e.response:
                print(f"  Response status: {e.response.status_code}")
                try:
                    error_content = e.response.json()
                    print(f"  Error details: {error_content}")
                except:
                    print(f"  Response content: {e.response.content[:200]}...")
            
            # If this is a "model not found" error, return None to signal caller
            # to try pulling the model or using an alternative
            if "not found" in str(e).lower():
                return None
    
    return all_embeddings if all_embeddings else None

def get_embeddings_via_generate(texts: List[str], model: str) -> Optional[List[List[float]]]:
    """
    Alternative approach to get embeddings using the generate endpoint
    with a special embedding prompt
    """
    embeddings = []
    
    for text in texts:
        try:
            url = f"{OLLAMA_BASE_URL}/api/generate"
            
            # This prompt asks the model to generate a vector representation
            embedding_prompt = f"""
            System: You are a vector embedding service. Convert text into a numerical vector.
            Convert the following text into a numerical vector representation with 384 dimensions.
            Only output the vector as a list of numbers separated by commas, surrounded by square brackets.
            Do not include any explanations or additional text.
            
            User text: "{text}"
            
            Vector: 
            """
            
            payload = {
                "model": model,
                "prompt": embedding_prompt,
                "stream": False
            }
            
            response = requests.post(url, json=payload, timeout=60)
            response.raise_for_status()
            result = response.json()
            
            # Extract response
            if "response" in result:
                vector_text = result["response"].strip()
                
                # Try to extract vector from the response
                try:
                    # Look for content between square brackets if present
                    if '[' in vector_text and ']' in vector_text:
                        vector_text = vector_text[vector_text.find('[')+1:vector_text.rfind(']')]
                    
                    # Split by commas and convert to floats
                    vector = [float(x.strip()) for x in vector_text.split(',') if x.strip()]
                    
                    # Only use if we got something that looks like a vector (at least 10 dimensions)
                    if len(vector) >= 10:
                        # Normalize the vector (important for embedding-like behavior)
                        norm = np.linalg.norm(vector)
                        if norm > 0:
                            vector = [v/norm for v in vector]
                        
                        embeddings.append(vector)
                        print(f"  ✓ Generated vector with {len(vector)} dimensions")
                    else:
                        print(f"  ✗ Response didn't contain valid vector (only {len(vector)} values)")
                except Exception as parse_error:
                    print(f"  ✗ Couldn't parse response as vector: {vector_text[:100]}... ({parse_error})")
            
            # Small delay
            time.sleep(1)
                
        except Exception as e:
            print(f"  ✗ Error in fallback method: {e}")
    
    return embeddings if embeddings else None

def find_best_model_for_embeddings(available_models, texts):
    """Find the best model for generating embeddings"""
    
    # 1. First, try specific embedding models that might be available
    embedding_candidates = [
        "bge-m3:567m",
        "bge-large",
        "all-minilm", 
        "nomic-embed-text",
        "mxbai-embed-large",
        "bge-base",
        "snowflake-arctic-embed"
    ]
    
    # 2. Then, try to match with available models
    available_embedding_models = []
    
    # Check direct matches
    for candidate in embedding_candidates:
        if candidate in available_models:
            available_embedding_models.append(candidate)
    
    # Check partial matches (models containing embedding keywords)
    for model in available_models:
        if any(keyword in model.lower() for keyword in ["embed", "bge", "minilm"]) and model not in available_embedding_models:
            available_embedding_models.append(model)
    
    print(f"Found {len(available_embedding_models)} potential embedding models from available models")
    
    # 3. Try each embedding model
    if available_embedding_models:
        for model in available_embedding_models:
            print(f"\nTrying embedding model: {model}")
            
            # Try both endpoints
            for endpoint in ["embeddings", "embed"]:
                print(f"  Trying endpoint: /api/{endpoint}")
                result = get_embeddings([texts[0]], model, endpoint=endpoint)
                
                if result:
                    print(f"✓ Success! Model {model} works with endpoint /api/{endpoint}")
                    return model, endpoint
    
    # 4. If no embedding model works, try general models for generate-based embeddings
    general_models = [m for m in available_models if any(name in m.lower() for name in 
                      ["llama", "mistral", "gemma", "phi", "mixtral", "qwen"])]
    
    if general_models:
        print("\nNo embedding-specific models worked. Trying general-purpose models:")
        
        for model in general_models[:5]:  # Limit to first 5 to avoid too many attempts
            print(f"\nTrying general model for embeddings: {model}")
            
            # Try generate-based embeddings
            result = get_embeddings_via_generate([texts[0]], model)
            
            if result:
                print(f"✓ Success! Model {model} works for generating embeddings via text generation")
                return model, "generate"
    
    # 5. If still no success, try pulling an embedding model
    for embedding_model in embedding_candidates[:2]:  # Try pulling at most 2 models
        print(f"\nNo existing models worked. Attempting to pull {embedding_model}...")
        success = pull_model(embedding_model)
        
        if success:
            # Wait a bit to ensure the model is loaded
            print("Waiting for model to be ready...")
            time.sleep(5)
            
            # Try both endpoints again
            for endpoint in ["embeddings", "embed"]:
                print(f"  Trying endpoint: /api/{endpoint}")
                result = get_embeddings([texts[0]], embedding_model, endpoint=endpoint)
                
                if result:
                    print(f"✓ Success! Newly pulled model {embedding_model} works with endpoint /api/{endpoint}")
                    return embedding_model, endpoint
    
    # 6. If nothing worked, return None
    return None, None

# Example usage
if __name__ == "__main__":
    # First check connection
    if not check_connection():
        print("Exiting due to connection issues")
        sys.exit(1)
    
    # Get available models
    available_models = get_available_models()
    
    # Sample texts to embed
    sample_texts = [
        "The sky is blue because of rayleigh scattering",
        "Grass is green because of chlorophyll",
        "Water is transparent but appears blue in large quantities",
        "Sunset appears red due to light scattering",
        "Leaves change color in autumn due to the breakdown of chlorophyll"
    ]
    
    print(f"\nFinding the best model and endpoint for embedding {len(sample_texts)} texts...")
    
    # Find the best model and endpoint for embeddings
    best_model, best_endpoint = find_best_model_for_embeddings(available_models, sample_texts)
    
    if not best_model:
        print("\n✗ Failed to find any working model for embeddings")
        print("Attempting to use the first available model with generate-based embeddings as last resort...")
        
        if available_models:
            best_model = available_models[0]
            best_endpoint = "generate"
        else:
            print("No models available at all. Cannot continue.")
            sys.exit(1)
    
    print(f"\nGenerating embeddings using model: {best_model} with endpoint: {best_endpoint}")
    start_time = time.time()
    
    # Generate embeddings based on the determined approach
    if best_endpoint == "generate":
        embeddings = get_embeddings_via_generate(sample_texts, best_model)
    else:
        embeddings = get_embeddings(sample_texts, best_model, endpoint=best_endpoint)
    
    end_time = time.time()
    
    if embeddings:
        print(f"\n✓ Successfully generated {len(embeddings)} embeddings in {end_time - start_time:.2f} seconds")
        
        # Show embedding dimensions
        print(f"Embedding dimension: {len(embeddings[0])}")
        
        # Calculate and print the average embedding norm
        norms = [np.linalg.norm(emb) for emb in embeddings]
        avg_norm = sum(norms) / len(norms)
        print(f"Average embedding L2 norm: {avg_norm:.4f}")
        
        # Compute similarity between embeddings
        if len(embeddings) >= 2:
            def cosine_similarity(a, b):
                return np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b))
            
            # Similarity between first two texts
            sim = cosine_similarity(embeddings[0], embeddings[1])
            print(f"\nSimilarity between first two texts: {sim:.4f}")
            
            # Find most similar pair
            most_similar = (0, 0, 0)  # (i, j, similarity)
            for i in range(len(embeddings)):
                for j in range(i+1, len(embeddings)):
                    similarity = cosine_similarity(embeddings[i], embeddings[j])
                    if similarity > most_similar[2]:
                        most_similar = (i, j, similarity)
            
            print(f"\nMost similar pair:")
            print(f"1: \"{sample_texts[most_similar[0]]}\"")
            print(f"2: \"{sample_texts[most_similar[1]]}\"")
            print(f"Similarity: {most_similar[2]:.4f}")
            
            # Save embeddings to file
            output_file = "embeddings_result.json"
            result_data = {
                "model": best_model,
                "method": best_endpoint,
                "embedding_dimension": len(embeddings[0]),
                "texts": sample_texts[:len(embeddings)],
                "embeddings": embeddings,
                "processing_time_seconds": end_time - start_time
            }
            
            with open(output_file, "w") as f:
                json.dump(result_data, f, indent=2)
            
            print(f"\nSaved complete embeddings to {output_file}")
    else:
        print("\nFailed to generate embeddings with any approach")
        print("\nPossible solutions:")
        print("1. Manually install an embedding model: ollama pull bge-m3:567m")
        print("2. Check if you have permission to pull models on this server")
        print("3. Set up a different Ollama instance with embedding support")