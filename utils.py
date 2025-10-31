import openai
from dotenv import load_dotenv
from litellm import completion
load_dotenv()

# --- LiteLLM wrapper for Gemini 2.5 Pro with system+user prompts ---
def call_llm(system_prompt: str, user_prompt: str, model: str = "gemini/gemini-2.5-pro", temperature: float = 0, response_format=None):

    kwargs = {}
    if response_format is not None:
        kwargs["response_format"] = response_format

    response = completion(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=temperature,
        **kwargs,
    )
    # LiteLLM returns an OpenAI-compatible response schema
    content = response["choices"][0]["message"].get("content", "")
    usage = response.get("usage", {})
    return content, usage.get("prompt_tokens", 0), usage.get("completion_tokens", 0)