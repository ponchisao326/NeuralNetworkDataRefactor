from src.config import Config
from src.reporting.html_generator import HTMLGenerator
from src.pipelines.battles import BattlesPipeline
from src.pipelines.breeding import BreedingPipeline
from src.pipelines.captures import CapturesPipeline
from src.pipelines.commands import CommandsPipeline
from src.pipelines.deaths import DeathsPipeline
from src.pipelines.economy import EconomyPipeline
from src.pipelines.raids import RaidsPipeline
from src.pipelines.released import ReleasedPipeline
from src.pipelines.sessions import SessionsPipeline
from src.pipelines.snapshots import SnapshotsPipeline


def main():
    """
    Main entry point for the ETL orchestration.
    Instantiates all pipelines, runs them, aggregates viz data, and builds the report.
    """
    print("--- INITIALIZING PIXELMON AI DATA PIPELINES ---")
    Config.ensure_dirs()

    # Instantiate all pipelines
    pipelines = [
        BattlesPipeline(),
        BreedingPipeline(),
        CapturesPipeline(),
        CommandsPipeline(),
        DeathsPipeline(),
        EconomyPipeline(),
        RaidsPipeline(),
        ReleasedPipeline(),
        SessionsPipeline(),
        SnapshotsPipeline()
    ]

    consolidated_viz_data = {}

    # Execute Pipelines
    for pipeline in pipelines:
        try:
            viz_data = pipeline.run()
            if viz_data:
                consolidated_viz_data[pipeline.output_name] = viz_data
        except Exception as e:
            print(f"[CRITICAL ERROR] Pipeline {pipeline.output_name} failed: {e}")
            import traceback
            traceback.print_exc()

    # Generate Reporting
    print("--- GENERATING FINAL REPORT ---")
    reporter = HTMLGenerator()
    reporter.generate_report(consolidated_viz_data)

    print("--- PROCESS COMPLETE ---")


if __name__ == "__main__":
    main()