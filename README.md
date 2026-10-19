# CS5224 Cloud Computing — Lab 2

## Overview

A full-stack web application that calculates and displays the number of followers and followees for each user in the provided Twitter dataset.

## Architecture

- **Front-End:** HTML, CSS and JavaScript, hosted on Amazon S3.
- **Back-End:** AWS Lambda (Python) and Amazon API Gateway, providing REST APIs.
- **Data Analysis:** Apache Spark running on Amazon EMR Serverless.
- **Storage:** Amazon S3 stores the input dataset and computed JSON results.

## Data Processing

1. Read the supplied `twitter_combined.txt` from S3.
2. Remove duplicate relationships using Spark's `distinct()`.
3. Generate follower and followee contributions using `flatMap()`.
4. Aggregate counts for each user using `reduceByKey()`.
5. Save the results to S3 as JSON files.
6. Retrieve and display the results through the backend API.

## Source Files

- `index.html` — Front-end webpage.
- `lambda_function.py` — Lambda backend API.
- `twitter_counts.py` — Spark data analysis pipeline.

## Results

| User ID | Followers | Followees |
|---|---:|---:|
| 214328887 | 180 | 214 |
| 107830991 | 180 | 157 |

The Spark job processed 81,306 unique users.
