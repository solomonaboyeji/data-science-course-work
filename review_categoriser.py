import asyncio
import datetime
import os
from langchain_community.chat_models.ollama import ChatOllama
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from concurrent.futures import ProcessPoolExecutor, as_completed

import pandas as pd

import base64

from dotenv import load_dotenv
from pathlib import Path
import os
from loguru import logger

load_dotenv()
base_dir_str = "./"

configured_model_name = "mistral:7b"
configured_temp = "1"

configured_model_name = os.getenv("MODEL_NAME", "llama2")
configured_temp = os.getenv("MODEL_TEMP", "1")
host_ip = os.getenv("REVIEW_MODEL_HOST_IP", None)

ollama_username = os.getenv("OLLAMA_USERNAME", None)
ollama_password = os.getenv("OLLAMA_PASSWORD", None)

# Encode the username and password
credentials = f"{ollama_username}:{ollama_password}"
credentials_b64 = base64.b64encode(credentials.encode()).decode()

loaded_model = ChatOllama(
    model=configured_model_name,
    template=configured_temp,
    base_url=f"{host_ip}",
    num_gpu=-1,
    num_thread=3,
    headers={"Authorization": f"Basic {credentials_b64}"},
    keep_alive=os.getenv("KEEP_MODEl_ALIVE", "5m"),
)


def get_base_path():
    _path = Path(f"{base_dir_str}")

    if not _path.exists():
        logger.info(f"{base_dir_str} does not exist, creating...")
        _path.mkdir()
        # raise FileNotFoundError(f"{base_dir_str} does not exist.")

    variables_loaded = load_dotenv(f"{_path.absolute()}/.env")
    if not variables_loaded:
        logger.warning(
            "env variables are not loaded, script might not work as expected."
        )
    else:
        logger.success("env variables loaded.")
    return _path


# @retry(wait=5, stop=stop_after_attempt(2))
def categorise_review(review_content, index: int, total: int):
    print(f"Review: {index}/{total}")

    categories = [
        "SHIPPING",
        "USER-EXPERIENCE",
        "CUSTOMER-SERVICE",
        "SECURITY",
        "PRIVACY",
    ]
    template_str = """
  You are an expert in reviewing reviews written by users of an e-commerce app, your task is to categorise each review into one of the following categories {categories}

  You should only reply with maximum of 1 category without explaining why you made that decision.

  Task: Categorise this review:

  <Review Start> {review} <Review End>.

  Check that your final answer only has one of these {categories} and no additional text.

  """

    prompt = ChatPromptTemplate.from_template(template_str)

    chain = prompt | loaded_model | StrOutputParser()

    category = chain.invoke({"review": review_content, "categories": categories})

    return str(category), index, total


async def run_categoriser():

    base_dir_path = get_base_path()

    # Define the path to your CSV file
    csv_file_path = f"{base_dir_path}/app_views_latest.csv"
    # Read the CSV file into a DataFrame
    df = pd.read_csv(csv_file_path)

    processed_csv_file_path = (
        f"{base_dir_path}/{datetime.datetime.now().microsecond}.csv"
    )
    processed_csv_file_path = csv_file_path

    total = len(df)
    num_cores = 4
    start = 1634
    end = 2173
    print("num_cores", num_cores)
    with ProcessPoolExecutor(max_workers=num_cores) as executor:
        futures = {
            executor.submit(categorise_review, row["content"], index, total): (
                row["content"],
                index,
                total,
            )
            for index, row in df[start:end].iterrows()
        }
        print("Number of futures", len(futures), processed_csv_file_path)

        for future in as_completed(futures):
            if future.result():
                category, index, _ = future.result()

                # Update DataFrame with category
                df.at[index, "category"] = category

                # Save the DataFrame to a CSV file
                if Path(processed_csv_file_path).exists():
                    os.remove(processed_csv_file_path)
                df.to_csv(processed_csv_file_path)


# Run the main async function
if __name__ == "__main__":
    asyncio.run(run_categoriser())
