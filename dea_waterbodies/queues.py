"""Make and destroy SQS queues for Waterbodies AWS jobs.

Matthew Alger
Geoscience Australia
2021
"""

import json

import boto3
import click


def verify_name(name):
    if not name.startswith('waterbodies_'):
        raise click.ClickException(
            'Waterbodies queues must start with waterbodies_')


@click.command()
@click.argument('name')
@click.option(
    '--timeout', type=int,
    help='Visibility timeout in seconds',
    default=2 * 60)
@click.option(
    '--deadletter', default=None,
    help='Name of deadletter queue')
@click.option(
    '--retries', type=int,
    help='Number of retries',
    default=5)
def make(name, timeout, deadletter, retries):
    """Make a queue."""
    verify_name(name)

    sqs = boto3.resource('sqs')

    redrive_policy = {
        'maxReceiveCount': retries,
    }
    if deadletter:
        # Get ARN from queue name.
        deadletter_queue = sqs.get_queue_by_name(
            QueueName=deadletter,
        )
        deadletter_arn = deadletter_queue.attributes['QueueArn']
        redrive_policy['deadLetterTargetArn'] = deadletter_arn

    queue = sqs.create_queue(
        QueueName=name,
        RedrivePolicy=json.dumps(redrive_policy),
        VisibilityTimeout=timeout)

    return queue.attributes['QueueArn']


@click.command()
@click.argument('name')
def delete(name):
    """Delete a queue."""
    verify_name(name)
    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(name)
    arn = queue.attributes['QueueArn']
    queue.delete()
    return arn
