# Custom Operator for DL jobs

## Overview

This project is a proof of concept of a Python Kubernetes operator for deep learning jobs. The project was inspired by Kuberflow's [tf-operator](https://github.com/kubeflow/tf-operator) and then by the work done to implement the [mx-operator](https://github.com/StefanoFioravanzo/mx-operator).

**Supported frameworks**:

- Tensorflow
- MXNet

The operator was build to be general purpose, so adding any new deep learning frameworks should be straightforward. The operator works best with deep learning framework that can setup a cluster just by reading environment variables. In this way one just needs to extend the standard base class providing the framework specific env variables and container parameters.

## Structure

```
.
├── controller.py
├── crds
│   ├── crd
│   │   └── mxjob.yml
│   └── mxjob_test.yml
├── dl_job.py
├── logging.ini
├── main.py
├── replica.py
└── settings
    └── settings.py
```

#### `DLOperator`

Defined in `controller.py`, the `DLOperator` object is tasked to continuously watch for new job requests by registering to the new custom resources stream.

Once the Operator receives a new unregistered job, it creates a new `DLJob`.

#### `DLJob`

Object responsible to maintain the state of the provided job spec with respect to the actual cluster state. This constant cycle of checking the current state and reconciling it with the desired state is the focal point of any custom Kubernetes controller. 

`DLJob` is an abstract class that must be extended with the specific deep learning framework details.

A correct subclass must provide the following class variables:

- `job_type`: crd name (e.g. _MXJob_)
- `replica_types`: list with the names of the types of replicas (e.g. _scheduler_, _master_, ...)
- `container_properties`: Dictionary with container properties (e.g. open ports, mount volume paths)

Also, the following abstract methods must be implemented:

- `get_environment_variables()`: This method must return a dictionary of environment variables to be injected into the container.

#### `Replica`

A `Replica` object encapsulates all the logic related to one running pod. One replica manages the reconciliation process, from pod-service creation until its death.

## TODO

- [ ] Allow network communication via hostnames
- [ ] Monitor process exit statuses and react accordingly
- [ ] Improve job status and result reporting