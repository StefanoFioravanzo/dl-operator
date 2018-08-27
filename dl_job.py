import os
import json
from abc import ABC, abstractmethod
import logging
import settings.settings as settings

from kubernetes import client
from replica import Replica

logger = logging.getLogger(os.path.basename(__file__))


class DLJob(ABC):
    """
    Abstract class which defines a distributed training job
    """

    # TODO: How to force redefinition of these two variables?
    job_type = None
    replica_types = list()
    container_properties = {}

    def __init__(self, name, spec):
        # check the class was properly subclassed
        if self.job_type is None or \
           type(self.replica_types) != list or \
           len(self.replica_types) == 0:
            raise Exception("Need to subclass DLJob with custom type and define job_type class variable")

        self.job_name = name
        self.spec = spec
        self.replicas = list()
        # TODO: Initialize this in one single place
        self.core_v1_client = client.CoreV1Api()

        self.clean_up_pods()
        self.create_replicas()
        self.reconcile()

    def create_replicas(self):
        logger.info("create replicas")
        # create the replica objects
        for replica_spec in self.spec:
            # create n replicas for this type of replica
            for i in range(0, replica_spec['replicas']):
                # create new replica
                self.container_properties['env'] = self.get_environment_variables(replica_spec, i)
                self.replicas.append(Replica(
                    uid=i,
                    replica_name=self.generate_replica_name(replica_spec["replicaType"].casefold(), self.job_name, i),
                    replica_type=replica_spec["replicaType"],
                    job_name=self.job_name,
                    template=replica_spec["template"],
                    # define arbitrary container specific parameters
                    **self.container_properties
                    ))

    def validate_spec(self):
        # validate replica type
        for replica_spec in self.spec['replicas']:
            if replica_spec['replica_type'].casefold() not in self.replica_types:
                logger.error(
                    f"Job {self.job_name} has invalid spec. {replica_spec['replica_type']} "
                    f"replica type not valid.")
            # standardize string
            replica_spec['replica_type'] = replica_spec['replica_type'].casefold()

    # @property
    # @abstractmethod
    # def job_type(cls):
    #     """
    #     Defines the name of the Kubernetes job
    #     :return: A string of the name of the job
    #     """
    #     pass
    #
    # @property
    # @abstractmethod
    # def replica_types(cls):
    #     """
    #     A list with the names of the possible replicas admitted by the job
    #     :return: List with the names of the replicas
    #     """
    #     pass

    @abstractmethod
    def get_environment_variables(self, replica_spec, replica_index):
        """
        Return env variables to be set for a specific replica
        :param replica_spec: The spec of the replica to be crated
        :param replica_index: The uid of the replica to be created,
            based on how many replicas need to be created from a template
            (see the `replicas` field of the spec)
        :return: A dictionary of env variables
        """
        pass

    def reconcile(self):
        logger.info(f"Reconcile Job {self.job_name}")
        for r in self.replicas:
            r.reconcile()

    def number_of_replicas(self, replica_type):
        for r in self.spec:
            if r["replicaType"] == replica_type:
                return r["replicas"]
        logger.error(f"Replica {replica_type} not found")

    def get_replica_spec(self, replica_type):
        for s in self.spec['replicas']:
            if s['replicaType'] == replica_type:
                return s

    def clean_up(self):
        for r in self.replicas:
            r.clean_up()

    def clean_up_pods(self):
        """
        Delete pods that match the selection job_name=self.job_name.
        This function is called every time a new job is created to delete any previous pod
        associated to the same job name. This avoid collisions and unpleasant behaviors.
        """
        api_instance = client.CoreV1Api()
        logger.info(f"Deleting pods matching {self.job_name} job name")
        selector = f"job_name={self.job_name}"
        pod_list = api_instance.list_namespaced_pod(settings.NAMESPACE, label_selector=selector)
        if len(pod_list.items) == 0:
            return
        logger.info(f"Deleting {len(pod_list.items)} pods.")
        pod_names = list()
        for pod in pod_list.items:
            logger.info(f"Deleting pod {pod.metadata.name}.")
            pod_names.append(pod.metadata.name)
            api_instance.delete_namespaced_pod(
                pod.metadata.name,
                settings.NAMESPACE,
                body=client.V1DeleteOptions())

        # wait for the resources to be deleted
        while len(api_instance.list_namespaced_pod(settings.NAMESPACE, label_selector=f"pod_name in ({pod_names})").items):
            pass

    @staticmethod
    def generate_replica_name(*args):
        return "-".join(map(str, list(args))).casefold()


class MXJob(DLJob):

    job_type = "MXJob"
    replica_types = ['scheduler', 'server', 'worker']
    container_properties = {
        "ports": [9000],
        "volumes": []
    }
    mx_port = 9000

    def __init__(self, name, spec):
        super(MXJob, self).__init__(name, spec)

    # @property
    # def job_type(cls):
    #     return "MXJob"
    #
    # @property
    # def replica_types(cls):
    #     return ['scheduler', 'server', 'worker']

    def validate_spec(self):
        super(MXJob, self).validate_spec()

    def get_environment_variables(self, replica_spec, replica_index):
        env = {
            # define later
            "DMLC_ROLE": replica_spec['replicaType'].casefold(),
            # define later
            "DMLC_PS_ROOT_URI": self.generate_replica_name("scheduler", self.job_name),
            "DMLC_PS_ROOT_PORT": self.mx_port,
            # auto conversion to str not supported by client for safety reasons
            "DMLC_NUM_SERVER": str(self.number_of_replicas("SERVER")),
            "DMLC_NUM_WORKER": str(self.number_of_replicas("WORKER")),
            "PS_VERBOSE": "2"
        }
        return env


class TFJob(DLJob):

    job_type = "TFJob"
    replica_types = ['ps', 'worker']
    container_properties = {
        "ports": [2222]
    }
    tf_port = 2222

    def __init__(self, name, spec):
        super(TFJob, self).__init__(name, spec)

    # def job_type(self):
    #     return "TFJob"
    #
    # def replica_types(self):
    #     return ['ps', 'worker']

    def validate_spec(self):
        super(TFJob, self).validate_spec()
        # TODO: Validate the number of identical replicaTypes = 1 (e.g. Just one PS specification)

    def get_cluster_config(self):
        cluster_config  = dict()
        # iterate over the number of replicas to give unique name
        cluster_config['ps'] = [
            "{}{}.{}".format("ps", i, self.tf_port)
            for i in range(self.number_of_replicas("ps"))
        ]
        cluster_config['worker'] = [
            "{}{}.{}".format("worker", i, self.tf_port)
            for i in range(self.number_of_replicas("worker"))
        ]
        return cluster_config

    def get_environment_variables(self, replica_spec, replica_index):
        tf_config = dict()
        tf_config['cluster'] = self.get_cluster_config()
        tf_config['task'] = {
            "type": replica_spec['replicaType'],
            "index": replica_index
        }
        env = {
            "TF_CONFIG": json.dumps(tf_config)
        }
        return env
