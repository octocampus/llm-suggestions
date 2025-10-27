from typing import List
from app.core.logging import logger
from app.llm_suggestions.llm_client import LLMClient
from app.llm_suggestions.prompts import SYSTEM_PROMPT, build_user_prompt
from app.model.llm_sugg_models import DataQualityRule, SuggestionResponse


class LLMSuggestionsService:

    def __init__(self):
        self.llm_client = LLMClient()
        logger.info(
            f"Initialized LLMSuggestionsService with {self.llm_client.provider}/{self.llm_client.model}"
        )

    def generate_suggestions(
        self, table_info: dict, sample_rows: List[dict]
    ) -> List[DataQualityRule]:

        schema_name = table_info.get("schema_name", "unknown")
        table_name = table_info.get("table_name", "unknown")

        logger.info(
            f"Generating DQ suggestions for {schema_name}.{table_name} "
            f"using {self.llm_client.model} ({len(sample_rows)} sample rows)"
        )

        user_prompt = build_user_prompt(table_info, sample_rows)

        try:
            response = self.llm_client.generate(SYSTEM_PROMPT, user_prompt)
        except Exception as e:
            logger.error(f"LLM generation failed for {schema_name}.{table_name}: {e}")
            raise
        # Parse response
        try:
            rules_json = self.llm_client.parse_json_response(response)
        except ValueError as e:
            logger.error(f"Failed to parse LLM response as JSON: {e}")
            logger.debug(f"Raw LLM response: {response[:500]}...")
            raise
        # Convert to Pydantic models
        rules = []
        for idx, rule_data in enumerate(rules_json):
            try:
                rule = DataQualityRule(**rule_data)
                rules.append(rule)
            except Exception as e:
                logger.warning(f"Failed to parse rule #{idx+1}: {e}, data: {rule_data}")
                continue

        logger.info(
            f"Successfully generated {len(rules)} DQ issues for {schema_name}.{table_name}"
        )
        if rules:
            logger.debug(
                f"Sample issue: {rules[0].column} - {rules[0].severity} ({len(rules[0].issues)} problems)"
            )

        return rules

    def generate_suggestions_response(
        self,
        source_key: str,
        schema_name: str,
        table_name: str,
        columns: List[dict],
        sample_rows: List[dict],
    ) -> SuggestionResponse:

        table_info = {
            "source_key": source_key,
            "schema_name": schema_name,
            "table_name": table_name,
            "columns": columns,
        }

        # Generate rules using enhanced prompts
        rules = self.generate_suggestions(table_info, sample_rows)

        return SuggestionResponse(
            source_key=source_key,
            schema_name=schema_name,
            table_name=table_name,
            rules=rules,
            row_count_analyzed=len(sample_rows),
            model_used=self.llm_client.model,
            metadata={
                "column_count": len(columns),
                "provider": self.llm_client.provider,
            },
        )
