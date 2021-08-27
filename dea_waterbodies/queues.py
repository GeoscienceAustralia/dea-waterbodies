"""Make and destroy SQS queues for Waterbodies AWS jobs.

Matthew Alger
Geoscience Australia
2021
"""

import json
import logging

import boto3
from botocore.config import Config
import click


logger = logging.getLogger(__name__)


def get_queue(queue_name: str):
    """
    Return a queue resource by name, e.g., alex-really-secret-queue

    Cribbed from odc.algo.
    """
    sqs = boto3.resource("sqs")
    queue = sqs.get_queue_by_name(QueueName=queue_name)
    return queue



def verify_name(name):
    if not name.startswith('waterbodies_'):
        raise click.ClickException(
            'Waterbodies queues must start with waterbodies_')


@click.group(invoke_without_command=False)
def cli():
    pass


@cli.command()
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

    sqs = boto3.client('sqs', config=Config(
        retries={
            'max_attempts': retries,
        }))

    attributes = dict(
            VisibilityTimeout=str(timeout))
    if deadletter:
        # Get ARN from queue name.
        deadletter_queue = sqs.get_queue_by_name(
            QueueName=deadletter,
        )
        deadletter_arn = deadletter_queue.attributes['QueueArn']
        attributes['RedrivePolicy'] = json.dumps(
            {'deadLetterTargetArn': deadletter_arn})

    queue = sqs.create_queue(
        QueueName=name,
        Attributes=attributes)

    assert queue
    return 0


@cli.command()
@click.argument('name')
def delete(name):
    """Delete a queue."""
    verify_name(name)
    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName=name)
    arn = queue.attributes['QueueArn']
    queue.delete()
    return arn


@cli.command()
@click.option('--txt', type=click.Path(), required=True,
              help='REQUIRED. Path to TXT file to push to queue.')
@click.option('--queue', required=True,
              help='REQUIRED. Queue name to push to.')
def push_to_queue(txt, queue):
    """
    Push lines of a text file to a SQS queue.
    """
    # Cribbed from datacube-alchemist
    alive_queue = get_queue(queue)

    def post_messages(messages, count):
        alive_queue.send_messages(Entries=messages)
        logger.info(f"Added {count} messages...")
        return []

    count = 0
    messages = []
    logger.info("Adding messages...")
    with open(txt) as file:
        ids = [line.strip() for line in file]
    logger.debug(f'Adding IDs {ids}')
    for id_ in ids:
        message = {
            "Id": str(count),
            "MessageBody": str(id_),
        }
        messages.append(message)

        count += 1
        if count % 10 == 0:
            messages = post_messages(messages, count)

    # Post the last messages if there are any
    if len(messages) > 0:
        post_messages(messages, count)


if __name__ == "__main__":
    cli()
