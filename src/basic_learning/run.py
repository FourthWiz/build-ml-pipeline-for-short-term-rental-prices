#!/usr/bin/env python
"""
Data Cleaning Step
"""
import argparse
import logging
import wandb
import pandas as pd


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_learning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    artifact_local_path = run.use_artifact(args.input_artifact).file()
    min_price = args.min_price
    max_price = args.max_price

    df = load_file(artifact_local_path)
    logger.info("Downloaded artifact")
    
    # Data cleaning
    df = clean_data(df, min_price, max_price)
    logger.info("Data cleaned")

    # Save the cleaned data
    save_file(df, run, args)
    run.finish()

    
def load_file(file_path):
    return pd.read_csv(file_path)

def clean_data(df, min_price, max_price):
    idx = df['price'].between(min_price, max_price)
    df = df[idx].copy()
    # Convert last_review to datetime
    df['last_review'] = pd.to_datetime(df['last_review'])
    #df.drop(columns=["Unnamed: 0"], inplace=True)
    return df

def save_file(df, run, args):
    df.to_csv("cleaned_data.csv", index=False)
    artifact = wandb.Artifact(
        args.output_artifact, type=args.output_type, description=args.output_description
    )
    artifact.add_file("cleaned_data.csv")
    run.log_artifact(artifact)
    logger.info("Saved cleaned data")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="Input artifact",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help="Output artifact",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="Output type",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help="Output description",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="Minimum price",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help="Maximum price",
        required=True
    )


    args = parser.parse_args()

    go(args)
