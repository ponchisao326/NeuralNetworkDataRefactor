from abc import ABC, abstractmethod
import pandas as pd
import json
import ast
import os
from typing import Dict, Any, Optional
from src.config import Config
from src.connectors.api_client import APIClient


class BaseDataPipeline(ABC):
    """
    Abstract Base Class implementing the Template Method pattern for ETL operations.

    This class serves as the architectural skeleton for all data processing pipelines.
    It enforces a strict sequence of operations (Extract -> Clean -> Feature Engineering -> Save -> Report)
    while allowing concrete subclasses to implement domain-specific logic.

    Attributes:
        action_type (str): The API event identifier (e.g., 'BATTLE_END').
        output_name (str): The logical name for output files (e.g., 'battles').
        raw_path (str): File path for storing unprocessed data.
        clean_path (str): File path for storing the final engineered dataset.
    """

    def __init__(self, action_type: str, output_name: str):
        """
        Initializes the pipeline paths based on the global configuration.

        Args:
            action_type: The specific event string used to query the API.
            output_name: A simplified name used for naming output CSVs and keys.
        """
        self.action_type = action_type
        self.output_name = output_name
        self.raw_path = os.path.join(Config.RAW_DIR, f"dataset_{action_type}_raw.csv")
        self.clean_path = os.path.join(Config.CLEAN_DIR, f"dataset_{output_name}_clean.csv")

    def run(self) -> Dict[str, Any]:
        """
        Executes the Template Method sequence.

        This method acts as the orchestrator, calling the lifecycle methods in a
        specific order to ensure data integrity and reproducibility.

        Returns:
            Dict[str, Any]: A dictionary containing serialized Plotly JSON objects
            ready for HTML embedding by the reporting service.
        """
        print(f"--- Starting Pipeline: {self.output_name} ---")

        # Retrieve data from the source. This step handles caching logic to
        # prevent unnecessary API calls during development or re-runs.
        df = self._extract_data()

        if df.empty:
            print(f"[WARN] No data found for {self.action_type}. Aborting pipeline execution.")
            return {}

        # The 'context_data' column typically contains nested JSON strings or
        # Python-string representations of dictionaries. This step normalizes
        # that structure into a flat DataFrame.
        df = self._clean_json_context(df)

        # Apply domain-specific logic, transformations, and metric calculations.
        # This is the primary extension point for concrete pipelines.
        df = self._feature_engineering(df)

        # Apply global standard transformations applicable to all datasets,
        # such as One-Hot Encoding for Server IDs.
        if 'server_id' in df.columns:
            server_dummies = pd.get_dummies(df['server_id'], prefix='server')
            df = pd.concat([df, server_dummies], axis=1)

        # Persist the processed data to the clean directory for future use
        # (e.g., Neural Network training).
        self._save_data(df)

        # Generate visualization artifacts for the HTML report.
        return self._generate_visualization_data(df)

    def _extract_data(self) -> pd.DataFrame:
        """
        Retrieves raw data, preferring a local cache if available.

        To ensure reproducibility in AI training, local files are prioritized.
        If the file is missing, it triggers an API fetch via the singleton client.

        Returns:
            pd.DataFrame: The raw dataset.
        """
        if os.path.exists(self.raw_path):
            print(f"[INFO] Loading cached raw data from {self.raw_path}")
            return pd.read_csv(self.raw_path)

        print(f"[INFO] Downloading fresh data for {self.action_type}")
        client = APIClient()
        data = client.fetch_data(self.action_type)

        if not data:
            return pd.DataFrame()

        df = pd.DataFrame(data)

        # Save raw data immediately to establish the cache.
        df.to_csv(self.raw_path, index=False)
        return df

    def _clean_json_context(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Parses and flattens the 'context_data' column.

        This method handles data heterogeneity, specifically coping with both
        standard JSON (double quotes) and Python string literals (single quotes)
        often introduced during CSV serialization.

        Args:
            df (pd.DataFrame): The DataFrame containing the raw 'context_data' column.

        Returns:
            pd.DataFrame: A new DataFrame with the JSON keys expanded into columns.
        """
        if 'context_data' not in df.columns:
            return df

        def safe_json_load(x: Any) -> Dict:
            """
            Attempt to parse a string into a dictionary safely.

            Strategy:
            1. Return as-is if already a dict.
            2. Try standard json.loads (strict JSON).
            3. Fallback to ast.literal_eval for single-quoted Python strings.
            4. Return empty dict on failure.
            """
            if isinstance(x, dict):
                return x
            if not isinstance(x, str):
                return {}

            try:
                # Primary attempt: Standard JSON
                return json.loads(x)
            except (json.JSONDecodeError, TypeError):
                try:
                    # Fallback attempt: Python string representation (e.g. {'key': 'val'})
                    return ast.literal_eval(x)
                except (ValueError, SyntaxError):
                    return {}

        print(f"[INFO] Expanding JSON context for {self.output_name}...")

        # Apply the safe parser to the context column
        df['context_data'] = df['context_data'].apply(safe_json_load)

        # Normalize the dictionary column into flat columns
        df_context = pd.json_normalize(df['context_data'])

        # Identify and remove columns that already exist in the main DataFrame
        # to avoid duplication conflicts during the join.
        cols_to_drop = df_context.columns.intersection(df.columns)
        if not cols_to_drop.empty:
            df_context = df_context.drop(columns=cols_to_drop)

        # Merge the expanded context back into the main DataFrame and remove the original column
        return df.join(df_context).drop(columns=['context_data'])

    def _save_data(self, df: pd.DataFrame) -> None:
        """
        Persists the transformed DataFrame to a CSV file.

        Args:
            df (pd.DataFrame): The final cleaned and engineered DataFrame.
        """
        df.to_csv(self.clean_path, index=False)
        print(f"[SUCCESS] Saved engineered data to {self.clean_path}")

    @abstractmethod
    def _feature_engineering(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Abstract method for applying domain-specific transformations.

        Concrete pipelines must implement this to calculate metrics (e.g., Win Rates,
        IV Percentages, Session Durations) specific to their event type.

        Args:
            df (pd.DataFrame): The expanded DataFrame.

        Returns:
            pd.DataFrame: The DataFrame with new feature columns added.
        """
        pass

    @abstractmethod
    def _generate_visualization_data(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Abstract method for generating reporting artifacts.

        Concrete pipelines must implement this to return Plotly figures serialized
        as JSON, which will be injected into the HTML report template.

        Args:
            df (pd.DataFrame): The processed DataFrame ready for analysis.

        Returns:
            Dict[str, Any]: A dictionary where keys are plot IDs and values are
            JSON strings of Plotly figures.
        """
        pass