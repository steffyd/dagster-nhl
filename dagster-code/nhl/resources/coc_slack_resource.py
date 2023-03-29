from dagster_slack import slack_resource
import os

coc_slack_resource = slack_resource.configured({"token":os.getenv('SLACK_TOKEN')})