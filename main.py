import click
from dotenv import load_dotenv

from data_sources import alpaca, yahoo
from lib import add_metadata, get_symbols

load_dotenv()


@click.command()
@click.option("--source", default="yahoo", help="Source to sync from (yahoo or alpaca)")
@click.option(
    "--skip-download", is_flag=True, help="Skip download and only consolidate"
)
def main(source, skip_download):
    if not skip_download:
        symbols = get_symbols()
        if source == "alpaca":
            alpaca.download(symbols)
        elif source == "yahoo":
            yahoo.download(symbols)

    if source == "alpaca":
        df = alpaca.consolidate()
    elif source == "yahoo":
        df = yahoo.consolidate()

    df = add_metadata(df)

    print(df.head())

    df.to_parquet(f"{source}.parquet", compression="snappy", row_group_size=100_000)


if __name__ == "__main__":
    main()
