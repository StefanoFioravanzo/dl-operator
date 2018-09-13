import os
import sys
import time
import json
import yaml
import atexit

from kubernetes import client, config, watch
from kubernetes.client.rest import ApiException

import dl_job
import settings.settings as settings

import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class DLOperator:

    def __init__(self):
        atexit.register(self.clean_up)

        if 'KUBERNETES_PORT' in os.environ:
            config.load_incluster_config()
        else:
            config.load_kube_config()
        self.configuration = client.Configuration()
        self.configuration.assert_hostname = False
        self.api_client = client.api_client.ApiClient(configuration=self.configuration)
        self.v1_client = client.ApiextensionsV1beta1Api(self.api_client)
        self.crd_client = client.CustomObjectsApi(self.api_client)

        self.jobs = {}

        # launch default clean up process
        self.clean_up()

    def clean_up(self):
        # delete all crds in the cluster
        # all jobs associated with the crds will be deleted as well
        logging.info("Deleting existing custom resources...")
        current_crds = ["{}.{}".format(x['spec']['names']['plural'], x['spec']['group'])
                        for x in self.v1_client.list_custom_resource_definition().to_dict()['items']]
        for c in current_crds:
            logger.info(f"Deleting {c}")
            self.v1_client.delete_custom_resource_definition(name=c, body=client.V1DeleteOptions())
        for k, v in self.jobs.items():
            v.clean_up()
        # wait a moment for the resource to be deleted
        time.sleep(2)

    def create_crd(self, crd_path):
        current_crds = [x['spec']['names']['kind'].lower() for x in
                        self.v1_client.list_custom_resource_definition().to_dict()['items']]
        logger.info(f"Creating {crd_path} CRD")
        with open(crd_path) as crd:
            body = yaml.load(crd)
            try:
                self.v1_client.create_custom_resource_definition(body)
            except ApiException as e:
                if json.loads(e.body)['reason'] == "AlreadyExists":
                    logger.info(f"Resource {body['metadata']['name']} already exists")
                    return
                raise e

    def create_dljob(self):
        # TODO: Make this work
        logger.info(f"Creating {dl_job} DLJOB")
        with open("test.json") as test:
            test0 = json.loads(test.read())
            try:
                self.crd_client.create_cluster_custom_object(settings.DOMAIN, "v1",
                                                             settings.CRD_NAME_PLURAL,
                                                             body=test0)
            except ApiException as e:
                logger.debug(json.loads(e.body))
                sys.exit(1)

    def update_crd(self, obj):
        metadata = obj.get("metadata")
        if not metadata:
            logger.info("No metadata in object, skipping: %s" % json.dumps(obj, indent=1))
            return
        name = metadata.get("name")
        namespace = metadata.get("namespace")
        spec = obj["spec"]

        # Update spec ...

        logger.info("Updating: %s" % name)
        logger.info("Updating: %s" % name)
        self.crd_client.replace_namespaced_custom_object(settings.DOMAIN, "v1", namespace, settings.CRD_NAME_PLURAL, name, obj)

    def watch_crd(self):
        logger.info("Waiting for DLJobs to come up...")
        resource_version = ''
        while True:
            stream = watch.Watch().stream(self.crd_client.list_cluster_custom_object, settings.DOMAIN, "v1", settings.CRD_NAME_PLURAL,
                                          resource_version=resource_version)
            for event in stream:
                obj = event["object"]
                operation = event['type']
                spec = obj.get("spec")
                if not spec:
                    continue
                metadata = obj.get("metadata")
                name = metadata['name']
                logger.info("Handling %s on %s" % (operation, name))

                if operation == "ADDED":
                    self.new_job(name=name, kind=obj.get("kind"), spec=spec)

    def new_job(self, name, kind, spec):
        # job does not exists. Create new job
        if name not in self.jobs:
            logger.debug(f"{name} not found in jobs. Create new one.")
            job_class = getattr(dl_job, kind)
            job = job_class(name, spec)
            self.jobs[name] = job
