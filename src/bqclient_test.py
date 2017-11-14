#!/usr/bin/python3

import argparse

from .bqclient import BqClient


def main():
    """
        BigQuery client test
    """

    # Parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("-a", "--auth_type", type=str, help="Authentication type", choices=['service_account'], default='service_account')
    parser.add_argument("-k", "--key", type=str, help="Json private key path", required=True)
    parser.add_argument("-q", "--query", type=str, help="Query to run", required=True)
    args = parser.parse_args()

    # BqClient instance
    bq = BqClient()

    # Authentication
    bq.setClient(args.key)
    print(" * BigQuery client instance:")
    print(bq.getClient())

    # Run query
    bq.runQuery(args.query)
    print(" * Query instance:")
    print(bq.getQueryJob())

    # Print the results.
    print(" * Query results:")
    for row in bq.readResult():
        print(row)


if __name__ == '__main__':
    main()
