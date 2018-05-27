#!/usr/bin/python3

import argparse

from .bqclient import BqClient


def run_test(query, key):
    """
        BigQuery client test
    """

    # BqClient instance
    bq = set_bq_instance()

    # Authentication
    set_client(bq, key)
    print(" * BigQuery client instance:")
    print(get_client(bq))

    # Run query
    run_query(bq, query)
    print(" * Query instance:")
    print(get_query_job(bq))

    # Print the results.
    print(" * Query results:")
    for row in read_results(bq):
        print(row)

    return True


def set_bq_instance():
    """
        Create instance of BqClient
    """

    return BqClient()


def set_client(bq, key):
    """
        Set BigQuery client
    """

    return bq.setClient(key)


def get_client(bq):
    """
        Get BigQuery client
    """

    return bq.getClient()


def run_query(bq, query):
    """
        Run query
    """

    return bq.runQuery(query)


def get_query_job(bq):
    """
        Get query job
    """

    return bq.getQueryJob()


def read_results(bq):
    """
        Read results
    """

    return [row for row in bq.readResult()]


def main():

    # Parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("-a", "--auth_type", type=str, help="Authentication type",
                        choices=['service_account'], default='service_account')
    parser.add_argument("-k", "--key", type=str,
                        help="Json private key path", required=True)
    parser.add_argument("-q", "--query", type=str,
                        help="Query to run", required=True)
    args = parser.parse_args()

    run_test(query=args.query,
             key=args.key)


if __name__ == '__main__':
    main()
