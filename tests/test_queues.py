"""Tests for dea_waterbodies.queues.

Matthew Alger
Geoscience Australia
2021
"""

import boto3
import click
from click.testing import CliRunner
from moto import mock_sqs
import pytest

from dea_waterbodies import queues


@mock_sqs
def test_make_queue():
    runner = CliRunner()
    sqs = boto3.resource('sqs')
    name = 'waterbodies_queue'
    res = runner.invoke(queues.make, [
        name,
    ], catch_exceptions=False)
    assert not res.exit_code, res.exception
    queue = sqs.get_queue_by_name(QueueName=name)
    assert queue


@mock_sqs
def test_make_queue_checks_name():
    """queues.make raises an exception if name doesn't have prefix."""
    runner = CliRunner()
    res = runner.invoke(queues.make, [
            'coastlines_'  # :)
        ])
    assert res.exit_code
