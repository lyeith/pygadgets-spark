#! <venv>/bin python3.6
# -*- coding: utf-8 -*-
"""
Created on Apr 02 14:58:36 2019

@author: david
"""
from pyspark import SparkContext, SparkConf, sql

import itertools
import sqlparse

from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword, DML


def is_subselect(parsed):
    if not parsed.is_group:
        return False
    for item in parsed.tokens:
        if item.ttype is DML and item.value.upper() == 'SELECT':
            return True
    return False


def extract_from_part(parsed):
    from_seen = False
    for item in parsed.tokens:
        if item.is_group:
            for x in extract_from_part(item):
                yield x
        if from_seen:
            if is_subselect(item):
                for x in extract_from_part(item):
                    yield x
            elif item.ttype is Keyword and item.value.upper() in ['ORDER', 'GROUP', 'BY', 'HAVING']:
                from_seen = False
                StopIteration
            else:
                yield item
        if item.ttype is Keyword and item.value.upper() == 'FROM':
            from_seen = True


def extract_table_identifiers(token_stream):
    for item in token_stream:
        if isinstance(item, IdentifierList):
            for identifier in item.get_identifiers():
                value = identifier.value.replace('"', '').lower()
                yield value
        elif isinstance(item, Identifier):
            value = item.value.replace('"', '').lower()
            yield value


def extract_tables(sql):
    # let's handle multiple statements in one sql string
    extracted_tables = []
    statements = list(sqlparse.parse(sql))
    for statement in statements:
        if statement.get_type() != 'UNKNOWN':
            stream = extract_from_part(statement)
            extracted_tables.append(set(list(extract_table_identifiers(stream))))
    return list(itertools.chain(*extracted_tables))


def exec_spark_sql(query, session):
    tables = extract_tables(query)
    tables = [e for e in tables if e[0] != ('(')]
    tokens = query.split()

    table_set = set()

    for elem in tables:
        v = elem.split(' ')[0]
        if len(v.split('.')) == 2:
            table_set.add(v)

    replacement_dct = {}

    for elem in table_set:
        v = elem.split('.')
        table_id = '_'.join(v)
        replacement_dct[elem] = table_id
        session.read.format('org.apache.spark.sql.cassandra') \
            .option('keyspace', v[0]).option('table', v[1]).load().createOrReplaceTempView(table_id)

    for k, elem in enumerate(tokens):
        if elem in replacement_dct:
            tokens[k] = replacement_dct[elem]

    query = ' '.join(tokens)

    return session.sql(query)


def connect_spark_sql(app_name='NAME_NOT_SPECIFIED', ENV='PROD'):
    SPARK_CASSANDRA_AUTH_USERNAME = {
        'DEV': 'ds_dev',
        'PROD': 'ds_spark'
    }

    SPARK_CASSANDRA_AUTH_PASSWORD = {
        'DEV': 'xNK3VFW6uWkX48xE',
        'PROD': 'LKV9FZzzPdqxHQc9'
    }

    SPARK_CASSANDRA_CONNECTION_HOST = {
        'DEV': '10.80.85.137',
        'PROD': '10.80.94.229,10.80.99.152,10.80.116.178'
    }

    spark_conf = SparkConf().setAppName(app_name)
    spark_conf.set('spark.cassandra.connection.host', SPARK_CASSANDRA_CONNECTION_HOST[ENV])
    spark_conf.set('spark.cassandra.auth.username', SPARK_CASSANDRA_AUTH_USERNAME[ENV])
    spark_conf.set('spark.cassandra.auth.password', SPARK_CASSANDRA_AUTH_PASSWORD[ENV])

    sc = SparkContext(conf=spark_conf)
    session = sql.SparkSession(sc)

    return sc, session
