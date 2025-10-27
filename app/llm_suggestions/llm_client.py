import json
from typing import List, Dict, Any
from app.core.logging import logger
from app.core.config import settings


class LLMClient:

    def __init__(self):
        self.provider = settings.llm_provider
        self.model = settings.llm_model
        self._client = None

    def _init_client(self):

        if self._client:
            return

        if self.provider == "groq":
            try:
                from groq import Groq

                self._client = Groq(api_key=getattr(settings, "groq_api_key", None))
                logger.info(f"Initialized Groq client with model {self.model}")
            except ImportError:
                raise ImportError(
                    "groq package not installed. Run: uv pip install groq"
                )

        elif self.provider == "openai":
            try:
                import openai

                self._client = openai.OpenAI(
                    api_key=getattr(settings, "openai_api_key", None)
                )
                logger.info(f"Initialized OpenAI client with model {self.model}")
            except ImportError:
                raise ImportError(
                    "openai package not installed. Run: pip install openai"
                )

        elif self.provider == "anthropic":
            try:
                import anthropic

                self._client = anthropic.Anthropic(
                    api_key=getattr(settings, "anthropic_api_key", None)
                )
                logger.info(f"Initialized Anthropic client with model {self.model}")
            except ImportError:
                raise ImportError(
                    "anthropic package not installed. Run: pip install anthropic"
                )

        elif self.provider == "ollama":
            try:
                import ollama

                self._client = ollama
                logger.info(f"Initialized Ollama client with model {self.model}")
            except ImportError:
                raise ImportError(
                    "ollama package not installed. Run: pip install ollama"
                )
        else:
            raise ValueError(f"Unsupported provider: {self.provider}")

    def generate(self, system_prompt: str, user_prompt: str) -> str:

        self._init_client()

        try:
            if self.provider == "groq":
                response = self._client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ],
                    temperature=0.3,
                    max_completion_tokens=8192,
                    top_p=1,
                )
                return response.choices[0].message.content

            elif self.provider == "openai":
                response = self._client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ],
                    temperature=0.3,  # Lower temp for more focused output
                    max_tokens=2000,
                )
                return response.choices[0].message.content

            elif self.provider == "anthropic":
                response = self._client.messages.create(
                    model=self.model,
                    max_tokens=2000,
                    temperature=0.3,
                    system=system_prompt,
                    messages=[{"role": "user", "content": user_prompt}],
                )
                return response.content[0].text

            elif self.provider == "ollama":
                response = self._client.chat(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ],
                )
                return response["message"]["content"]

        except Exception as e:
            logger.error(f"LLM generation failed: {e}")
            raise

    def parse_json_response(self, response: str) -> List[Dict[str, Any]]:
        """
        Parse JSON from LLM response

        Args:
            response: Raw LLM response text

        Returns:
            Parsed JSON as list of dicts
        """
        # Try to extract JSON from markdown code blocks
        if "```json" in response:
            start = response.find("```json") + 7
            end = response.find("```", start)
            response = response[start:end].strip()
        elif "```" in response:
            start = response.find("```") + 3
            end = response.find("```", start)
            response = response[start:end].strip()

        try:
            return json.loads(response)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON: {e}\nResponse: {response}")
            raise ValueError(f"LLM returned invalid JSON: {str(e)}")
