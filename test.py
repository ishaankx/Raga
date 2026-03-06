from google import genai
import os

# 1. Provide your API key here
api_key = os.getenv("GEMINI_API_KEY", "YOUR_API_KEY_HERE")

# 2. Initialize the client
client = genai.Client(api_key=api_key)

# 3. List and print available models that support content generation
print("Models supporting generateContent:")
print("-" * 35)
for model in client.models.list_models():
    # Check if 'generateContent' is in the supported generation methods
    if "generateContent" in model.supported_generation_methods:
        print(model.name)
