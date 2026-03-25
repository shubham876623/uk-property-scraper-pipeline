import os
import base64
import requests
from dotenv import load_dotenv

load_dotenv()

# Try to import OpenAI - handle both old and new versions
try:
    import openai
    # For newer versions (v1.0+)
    try:
        from openai import OpenAI
        api_key = os.getenv("OPENAI_API_KEY")
        if api_key:
            client = OpenAI(api_key=api_key)
            USE_NEW_API = True
        else:
            client = None
            USE_NEW_API = True
    except (ImportError, AttributeError):
        # For older versions (v0.28.0)
        openai.api_key = os.getenv("OPENAI_API_KEY")
        client = None
        USE_NEW_API = False
except ImportError:
    raise ImportError("OpenAI package not installed. Please install: pip install openai")

def extract_text_from_image(image_url):
    prompt = """
Extract the energy efficiency values from the given EPC rating chart.

The chart contains three key values:
1. Current Score (a numerical value)
2. Potential Score (another numerical value)
3. Rating (A-G scale based on the Current Score)

Rating Scale:
- A: 92+
- B: 81-91
- C: 69-80
- D: 55-68
- E: 39-54
- F: 21-38
- G: 1-20

Return the extracted values in **pure JSON format**, with no additional text, markdown, or formatting:

{
  "current_score": VALUE,
  "potential_score": VALUE,
  "rating": "LETTER"
}
    """

    try:
        # Step 1: Download the image from the URL
        response = requests.get(image_url, timeout=10)
        response.raise_for_status()
        image_bytes = response.content

        # Step 2: Convert to base64
        base64_image = base64.b64encode(image_bytes).decode("utf-8")

        # Step 3: Send to OpenAI using base64-encoded image
        if USE_NEW_API and client:
            # New OpenAI API (v1.0+)
            result = client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": prompt},
                    {"role": "user", "content": [
                        {"type": "text", "text": "Extract the EPC values from this image."},
                        {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{base64_image}"}}
                    ]}
                ],
                max_tokens=300
            )
            return result.choices[0].message.content
        else:
            # Old OpenAI API (v0.28.0)
            result = openai.ChatCompletion.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": prompt},
                    {"role": "user", "content": [
                        {"type": "text", "text": "Extract the EPC values from this image."},
                        {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{base64_image}"}}
                    ]}
                ],
                max_tokens=300
            )
            return result["choices"][0]["message"]["content"]

    except requests.exceptions.RequestException as e:
        raise ValueError(f"Failed to download image: {e}")
    except Exception as e:
        # Handle both old and new OpenAI API errors
        error_msg = str(e)
        if "openai" in error_msg.lower() or "api" in error_msg.lower():
            raise ValueError(f"OpenAI API error: {e}")
        raise ValueError(f"Error processing image: {e}")
