import os
from kubernetes import client

import settings.settings as settings

import logging
logger = logging.getLogger(os.path.basename(__file__))


class Replica:
    """
    This class defines the properties and the behavior of one replica in the cluster.
    """
    def __init__(self, uid, replica_name, replica_type, job_name, template, **kwargs):
        # environment_variables=None, volumes=None, ports=None
        self.uid = uid
        self.replica_name = replica_name
        self.replica_type = replica_type
        self.job_name = job_name
        self.template = template
        self.status = "None"

        # TODO: create a function to check these parameters
        self.container_params = kwargs
        # self.environment_variables = kwargs['environment_variables']
        # self.volumes = kwargs['volumes']
        # self.ports = kwargs['ports']

        self.api_instance = client.CoreV1Api()

    def reconcile(self):
        """Check the current status of the replica and match it against the desired state.
        """
        logger.info(f"Reconcile replica {self.replica_name} with type {self.replica_type}")
        # check if pod with current replica name already exists using selector with replica name
        # selector = client.V1LabelSelector(match_labels={"pod_name": self.replica_name})
        selector = f"pod_name={self.replica_name}"
        # returns a V1PodList object
        pod_list = self.api_instance.list_namespaced_pod(settings.NAMESPACE, label_selector=selector)

        if len(pod_list.items) == 0:
            self.create_replica()
        elif len(pod_list.items) > 1:
            # this should not happen. The operator should make sure that a single
            # pod with that label name is created.
            assert False
        else:
            # TODO: should check the status of the replica here
            pod = pod_list.items[0]
            assert pod.metadata.labels['pod_name'] == self.replica_name
            logger.info("Replica pod already exist. Checking the current status...")
            pass

    def create_replica(self):
        self.create_pod()
        # in case the pod needs to expose some ports
        if 'ports' in self.container_params:
            self.create_service(ports=self.container_params['ports'])

    def create_pod(self):
        pod = client.V1Pod()
        pod.metadata = client.V1ObjectMeta(
            name=self.replica_name,
            labels={"pod_name": self.replica_name,
                    "job_name": self.job_name})

        container_spec = self.template["spec"]["containers"][0]
        # add container properties to container spec
        # if 'env' in self.container_params:
        #     # add env variables to container_spec
        #     container_spec['env'] = [
        #         client.V1EnvVar(name=k, value=v)
        #         for k, v in self.container_params['env'].items()
        #     ]
        if 'volumes' in self.container_params:
            # add volumes spec to container
            # TODO: look at how to mount a volume (volumeMounts vs volumeDevices?)
            container_spec['volumeMounts'] = []

        # V1Container accepts keys defined in V1Container.attribute_map.
        # The current spec is defined with camel case keys, so we need to map back
        # to underscore keys.
        inv_map = {
            k: container_spec[v]
            for k, v in client.V1Container.attribute_map.items()
            if v in container_spec
        }
        pod_spec = client.V1PodSpec(containers=[client.V1Container(**inv_map)])

        pod.spec = pod_spec
        self.api_instance.create_namespaced_pod(namespace=settings.NAMESPACE, body=pod)
        logger.info(f"Created Pod for replica {self.replica_name} with container spec {inv_map}")

    def create_service(self, ports):
        """Creates an tandem network service for this replica.

            Args:
                ports: The ports to be exposed to the other replicas
        """
        service = client.V1Service()
        service.api_version = "v1"
        service.kind = "Service"
        service.metadata = client.V1ObjectMeta(name=f"{self.replica_name}")
        # Set Service object to target port on any Pod with that label.
        # The label selection allows Kubernetes to determine which Pod
        # should receive traffic when the service is used.
        spec = client.V1ServiceSpec()
        spec.selector = {"pod_name": self.replica_name}
        spec.ports = [
            client.V1ServicePort(name="dlport{}".format(i), port=port)
            for i, port in enumerate(ports)
        ]
        service.spec = spec
        self.api_instance.create_namespaced_service(namespace=settings.NAMESPACE, body=service)

    def clean_up(self):
        logger.info(f"Deleting pod {self.replica_name}...")
        selector = f"pod_name={self.replica_name}"
        pod_list = self.api_instance.list_namespaced_pod(settings.NAMESPACE, label_selector=selector)
        if len(pod_list.items) == 0:
            logging.warning(f"No pod with name {self.replica_name} found during clean up")
            return
        if len(pod_list.items) > 1:
            logging.warning(f"Multiple pods found with labels name {self.replica_name} found during clean up")
            assert False
        self.api_instance.delete_namespaced_pod(
            self.replica_name,
            settings.NAMESPACE,
            body=client.V1DeleteOptions())

    def get_uid(self):
        """Getter function for the `uid` of this replica

            Returns:
                Int. The unique `uid` of this replica

        """
        return self.uid
