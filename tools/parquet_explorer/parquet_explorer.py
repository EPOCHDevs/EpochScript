#!/usr/bin/env python3
"""
Interactive Parquet File Explorer
Loads parquet files from data_explorer output and provides interactive exploration.
"""

import os
import sys
from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq
import dtale
import argparse


def find_parquet_files(root_dir):
    """Recursively find all parquet files in directory."""
    parquet_files = []
    for root, dirs, files in os.walk(root_dir):
        for file in files:
            if file.endswith('.parquet') or file.endswith('.parquet.gzip'):
                full_path = os.path.join(root, file)
                # Get relative path for display
                rel_path = os.path.relpath(full_path, root_dir)
                parquet_files.append((rel_path, full_path))
    return parquet_files


def load_parquet(file_path):
    """Load parquet file into pandas DataFrame."""
    print(f"\nLoading {file_path}...")
    df = pd.read_parquet(file_path).set_index("index")
    print(f"Loaded {len(df)} rows, {len(df.columns)} columns")
    print(f"Columns: {', '.join(df.columns)}")
    return df


def main():
    parser = argparse.ArgumentParser(
        description='Interactive Parquet File Explorer',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Explore a specific directory
  python parquet_explorer.py /tmp/explorer_test/daily/

  # Explore a specific parquet file directly
  python parquet_explorer.py /tmp/explorer_test/daily/tables/1D/AAPL-Stocks.parquet.gzip

  # Use current directory
  python parquet_explorer.py .
        """
    )
    parser.add_argument(
        'path',
        nargs='?',
        default='.',
        help='Directory containing parquet files or path to specific parquet file'
    )
    parser.add_argument(
        '--port',
        type=int,
        default=8050,
        help='Port for web interface (default: 8050)'
    )

    args = parser.parse_args()

    path = Path(args.path)

    if not path.exists():
        print(f"Error: Path {path} does not exist")
        sys.exit(1)

    # Check if it's a specific parquet file
    if path.is_file() and (str(path).endswith('.parquet') or str(path).endswith('.parquet.gzip')):
        print(f"Loading parquet file: {path}")
        df = load_parquet(str(path))

        print("\n" + "="*80)
        print("Opening D-Tale interactive viewer...")
        print("="*80)
        print(f"\nWeb interface will open at: http://localhost:{args.port}")
        print("\nFeatures:")
        print("  - Interactive DataFrame viewer with sorting, filtering")
        print("  - Built-in plotting (line, bar, scatter, heatmap, etc.)")
        print("  - Column statistics and correlations")
        print("  - Query builder (SQL-like filtering)")
        print("  - Export to CSV/Excel")
        print("\nPress Ctrl+C to exit")
        print("="*80)

        # Launch D-Tale
        d = dtale.show(df, host='localhost', port=args.port, open_browser=True)
        d.open_browser()

        # Keep running
        import time
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n\nShutting down...")

    elif path.is_dir():
        # Find all parquet files in directory
        print(f"Searching for parquet files in: {path}")
        parquet_files = find_parquet_files(str(path))

        if not parquet_files:
            print(f"No parquet files found in {path}")
            sys.exit(1)

        print(f"\nFound {len(parquet_files)} parquet file(s):")
        for i, (rel_path, full_path) in enumerate(parquet_files, 1):
            file_size = os.path.getsize(full_path) / 1024 / 1024  # MB
            print(f"  [{i}] {rel_path} ({file_size:.2f} MB)")

        # If only one file, load it automatically
        if len(parquet_files) == 1:
            print(f"\nAutomatically loading the only file found...")
            df = load_parquet(parquet_files[0][1])
        else:
            # Let user choose
            while True:
                try:
                    choice = input(f"\nSelect file [1-{len(parquet_files)}] or 'q' to quit: ").strip()
                    if choice.lower() == 'q':
                        sys.exit(0)

                    idx = int(choice) - 1
                    if 0 <= idx < len(parquet_files):
                        df = load_parquet(parquet_files[idx][1])
                        break
                    else:
                        print(f"Please enter a number between 1 and {len(parquet_files)}")
                except ValueError:
                    print("Invalid input. Please enter a number or 'q'")
                except KeyboardInterrupt:
                    print("\n\nExiting...")
                    sys.exit(0)

        print("\n" + "="*80)
        print("Opening D-Tale interactive viewer...")
        print("="*80)
        print(f"\nWeb interface will open at: http://localhost:{args.port}")
        print("\nFeatures:")
        print("  - Interactive DataFrame viewer with sorting, filtering")
        print("  - Built-in plotting (line, bar, scatter, heatmap, etc.)")
        print("  - Column statistics and correlations")
        print("  - Query builder (SQL-like filtering)")
        print("  - Export to CSV/Excel")
        print("\nPress Ctrl+C to exit")
        print("="*80)

        # Launch D-Tale
        d = dtale.show(df, host='localhost', port=args.port, open_browser=True)
        d.open_browser()

        # Keep running
        import time
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n\nShutting down...")
    else:
        print(f"Error: {path} is neither a file nor a directory")
        sys.exit(1)


if __name__ == '__main__':
    main()
