import os
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
client = OpenAI(api_key=OPENAI_API_KEY)


def openai_chat_api_structured(
    messages, *, model="gpt-4.1", temperature=0, seed=42, response_format=None
):
    """
    Similar to openai_chat_api, but enforces a structured output
    using the Beta OpenAI API features for structured JSON output.
    """
    # enforces schema adherence with response_format
    completion = client.beta.chat.completions.parse(
        messages=messages,
        model=model,
        temperature=temperature,
        seed=seed,
        response_format=response_format,  # type: ignore
    )

    structured_response = completion.choices[0].message
    # Catch refusals
    if structured_response.refusal:
        raise ValueError(
            "OpenAI refused to complete input: " + structured_response.refusal
        )
    elif structured_response.parsed:
        return structured_response.parsed
    else:
        raise ValueError("No structured output or refusal was returned.")


def openai_chat_api(messages, *, model="gpt-4.1", temperature=0, seed=42):
    response = client.chat.completions.create(
        messages=messages, model=model, temperature=temperature, seed=seed
    )
    return response.choices[0].message.content
