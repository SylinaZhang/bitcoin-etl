#  MIT License
#
#  Copyright (c) 2020 Evgeny Medvedev, evge.medvedev@gmail.com
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in all
#  copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#  SOFTWARE.

from blockchainetl.jobs.exporters.console_item_exporter import ConsoleItemExporter
from blockchainetl.jobs.exporters.multi_item_exporter import MultiItemExporter


def create_item_exporters(outputs):
    split_outputs = [output.strip() for output in outputs.split(',')] if outputs else ['console']

    item_exporters = [create_item_exporter(output) for output in split_outputs]
    return MultiItemExporter(item_exporters)


def create_item_exporter(output):
    item_exporter_type = determine_item_exporter_type(output)
    if item_exporter_type == ItemExporterType.PUBSUB:
        from blockchainetl.jobs.exporters.google_pubsub_item_exporter import GooglePubSubItemExporter
        item_exporter = GooglePubSubItemExporter(
            item_type_to_topic_mapping={
                'block': output + '.blocks',
                'transaction': output + '.transactions',
                'ltransaction_input': output + '.transaction_input'
            },
            message_attributes=('item_id',))
    elif item_exporter_type == ItemExporterType.KINESIS:
        from blockchainetl.jobs.exporters.kinesis_item_exporter import KinesisItemExporter
        item_exporter = KinesisItemExporter(
            stream_name=output[len('kinesis://'):],
        )
    elif item_exporter_type == ItemExporterType.CONSOLE:
        item_exporter = ConsoleItemExporter()
    else:
        raise ValueError('Unable to determine item exporter type for output ' + output)

    return item_exporter

def determine_item_exporter_type(output):
    if output is not None and output.startswith('projects'):
        return ItemExporterType.PUBSUB
    if output is not None and output.startswith('kinesis://'):
        return ItemExporterType.KINESIS
    elif output is None or output == 'console':
        return ItemExporterType.CONSOLE
    else:
        return ItemExporterType.UNKNOWN


class ItemExporterType:
    PUBSUB = 'pubsub'
    KINESIS = 'kinesis'
    CONSOLE = 'console'
    UNKNOWN = 'unknown'
