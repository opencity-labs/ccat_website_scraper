# NOTE: using Cheshire Cat version 2

from cat import tool, hook, plugin, StrayCat
from cat.types import ChatResponse

from pydantic import BaseModel


class PluginSettings(BaseModel):
    favourite_language: str = "chinese"

@plugin
def settings_model():
    return PluginSettings


@tool
async def get_alphabet(language: str, include_numbers: bool, cat: StrayCat):
    """Get the alphabet for a specific language, optionally including numbers."""

    prompt = f"Provide the alphabet for the language {language}, in a compact format."
    if include_numbers:
        prompt += " Include numbers as well."
    
    alphabet = await cat.llm(prompt)
    return alphabet


@hook
async def before_cat_sends_message(chat_response: ChatResponse, cat: StrayCat):

    # load plugin settings
    settings = await cat.plugin.load_settings()
    favourite_language = settings.get("favourite_language")

    # translate last message to favourite language
    last_message = chat_response.messages[-1].content.text
    translation = await cat.llm(f"Translate to {favourite_language}: {last_message}")

    # append translation to chat response
    chat_response.messages.append(translation)
    return chat_response
