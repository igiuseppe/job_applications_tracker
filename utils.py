import openai

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
    return response.choices[0].message.content