import pandas as pd
import re
import plotly.graph_objects as go
from typing import Dict, Any
from src.pipelines.base_pipeline import BaseDataPipeline

class EconomyPipeline(BaseDataPipeline):
    """
    Pipeline for analyzing Global Trade System (GTS) transactions.

    Parses transaction descriptions to extract product details (Pokémon name, level)
    and analyzes market trends such as pricing correlations and server volume.
    """

    def __init__(self):
        super().__init__(action_type="GTS_TRANSACTION", output_name="economy")

    def _feature_engineering(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Parses item descriptions and normalizes price data.

        Extracts the Pokémon species and level from the raw description string
        (e.g., "Pikachu Lvl5") using regex.

        Args:
            df (pd.DataFrame): Raw transaction data.

        Returns:
            pd.DataFrame: DataFrame with extracted product names and numeric levels.
        """
        def parse_desc(row):
            """Helper to extract Name and Level from description string."""
            desc = str(row.get('description', ''))
            itype = row.get('itemType', 'ITEM')
            name = desc
            lvl = 0

            if itype == 'POKEMON':
                # Regex to extract "Name LvlX" pattern
                match = re.search(r'(.+)\s+Lvl(\d+)', desc)
                if match:
                    name = match.group(1).strip()
                    lvl = int(match.group(2))
            return pd.Series([name, lvl])

        # Apply parsing logic
        df[['product_name', 'level']] = df.apply(parse_desc, axis=1)

        # Ensure numeric types for price and level
        if 'price' in df.columns:
            df['price'] = pd.to_numeric(df['price'], errors='coerce').fillna(0)

        if 'listingDurationMs' in df.columns:
            df['hours_on_market'] = pd.to_numeric(df['listingDurationMs'], errors='coerce').fillna(0) / 3.6e6

        return df

    def _generate_visualization_data(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Generates economic market charts.

        Visualizations:
        1. Top 10 Most Expensive Pokémon.
        2. Price vs. Level Correlation Scatter Plot.
        3. Transaction Volume per Server.

        Args:
            df (pd.DataFrame): Processed economic data.

        Returns:
            Dict[str, Any]: JSON-serialized Plotly figures.
        """
        plots = {}

        # Filter only Pokémon sales for cleaner analysis
        pokemon_sales = df[df['itemType'] == 'POKEMON'].copy() if 'itemType' in df.columns else df.copy()

        # Visualization: Top 10 Most Expensive Species (Avg Price)
        if not pokemon_sales.empty and 'product_name' in pokemon_sales.columns and 'price' in pokemon_sales.columns:
            avg_prices = pokemon_sales.groupby('product_name')['price'].mean().sort_values(ascending=False).head(10)

            fig = go.Figure(data=[go.Bar(
                x=avg_prices.values.tolist(),
                y=avg_prices.index.tolist(),
                orientation='h',
                marker=dict(color='gold')
            )])
            fig.update_layout(
                title="Top 10 Most Expensive Pokémon (Avg Price)",
                xaxis_title="Average Price",
                yaxis=dict(autorange="reversed")
            )
            plots['top_expensive'] = fig.to_json()

        # Visualization: Price vs Level Correlation
        if not pokemon_sales.empty and 'level' in pokemon_sales.columns and 'price' in pokemon_sales.columns:
            scatter_df = pokemon_sales[['level', 'price', 'product_name']].dropna()

            fig2 = go.Figure(data=[go.Scatter(
                x=scatter_df['level'].tolist(),
                y=scatter_df['price'].tolist(),
                mode='markers',
                marker=dict(
                    size=10,
                    color=scatter_df['price'].tolist(),  # Color mapping by price
                    colorscale='Viridis',
                    showscale=True,
                    colorbar=dict(title="Price")
                ),
                text=scatter_df['product_name'].tolist(),  # Tooltip
                hovertemplate='<b>%{text}</b><br>Level: %{x}<br>Price: %{y}<extra></extra>'
            )])
            fig2.update_layout(
                title="Correlation: Price vs Level",
                xaxis_title="Level",
                yaxis_title="Price"
            )
            plots['price_scatter'] = fig2.to_json()

        # Visualization: Transactions Volume by Server
        if 'server_id' in df.columns:
            server_counts = df['server_id'].value_counts()

            fig3 = go.Figure(data=[go.Bar(
                x=server_counts.index.tolist(),
                y=server_counts.values.tolist(),
                marker_color='cornflowerblue'
            )])
            fig3.update_layout(
                title="Transactions Volume by Server",
                xaxis_title="Server ID",
                yaxis_title="Transaction Count"
            )
            plots['server_volume'] = fig3.to_json()

        return plots