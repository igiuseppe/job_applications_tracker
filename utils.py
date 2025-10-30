import openai
from dotenv import load_dotenv
from litellm import completion
load_dotenv()

LLM_MODEL = "gpt-4.1-nano"
temperature = 0

def call_llm(prompt: str, response_format=None, model=LLM_MODEL,temperature=temperature) -> str:
    client = openai.OpenAI()
    if response_format:
        response = client.beta.chat.completions.parse(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            response_format=response_format,
            temperature=temperature
        )
    else:
        response = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            temperature=temperature
        )
    input_tokens = response.usage.prompt_tokens
    output_tokens = response.usage.completion_tokens
    return response.choices[0].message.content,input_tokens,output_tokens

# --- LiteLLM wrapper for Gemini 2.5 Pro with system+user prompts ---
def call_llm_litellm(system_prompt: str, user_prompt: str, model: str = "gemini/gemini-2.5-pro", temperature: float = 0.2, response_format=None):

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

def truncate(text: str, max_chars: int) -> str:
    if text is None:
        return ""
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 1].rstrip() + "…"