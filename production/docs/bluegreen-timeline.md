# Blue/Green Timeline

PROD-02 / D-06 可观察演练证据（脚本 `production/scripts/run-bluegreen-drill.sh` 生成）。

- Started UTC: 2026-07-18T06:01:25Z
- Namespace: `flink`
- CR: `p03-vehicle-alert-bg`
- Blue image tag: `dev` → Green image tag: `dev-green`


## CRD

UTC: 2026-07-18T06:01:25Z

```
NAME                                         CREATED AT
flinkbluegreendeployments.flink.apache.org   2026-07-18T05:40:05Z
```

## Nodes

UTC: 2026-07-18T06:01:25Z

```
NAME       STATUS   ROLES           AGE   VERSION        INTERNAL-IP     EXTERNAL-IP   OS-IMAGE   KERNEL-VERSION                        CONTAINER-RUNTIME
orbstack   Ready    control-plane   82m   v1.34.8+orb1   192.168.139.2   <none>        OrbStack   7.0.11-orbstack-00360-gc9bc4d96ac70   docker://29.4.0
```

## Pre-transition status

UTC: 2026-07-18T06:01:26Z

### FlinkBlueGreenDeployment status

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkBlueGreenDeployment
metadata:
  annotations:
    meta.helm.sh/release-name: p03-vehicle-alert
    meta.helm.sh/release-namespace: flink
  creationTimestamp: "2026-07-18T05:59:29Z"
  generation: 1
  labels:
    app.kubernetes.io/managed-by: Helm
    app.kubernetes.io/name: p03-vehicle-alert
    app.kubernetes.io/part-of: flink-learning-lab
  name: p03-vehicle-alert-bg
  namespace: flink
  resourceVersion: "2653"
  uid: 7f19b8ab-0420-4706-8379-0be9b27126f1
spec:
  configuration:
    kubernetes.operator.bluegreen.abort.grace-period: 8 min
    kubernetes.operator.bluegreen.deployment-deletion.delay: 0ms
    kubernetes.operator.bluegreen.reconciliation.reschedule-interval: 15s
  template:
    spec:
      flinkConfiguration:
        execution.checkpointing.dir: s3://flink/checkpoints
        execution.checkpointing.interval: 30s
        execution.checkpointing.savepoint-dir: s3://flink/savepoints
        fs.s3a.access.key: flinklab
        fs.s3a.aws.credentials.provider: org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
        fs.s3a.connection.ssl.enabled: "false"
        fs.s3a.endpoint: http://host.docker.internal:9000
        fs.s3a.path.style.access: "true"
        fs.s3a.secret.key: flinklab123
        s3.access-key: flinklab
        s3.endpoint: http://host.docker.internal:9000
        s3.path.style.access: "true"
        s3.secret-key: flinklab123
      flinkVersion: v2_2
      image: flinklab/p03-vehicle-alert:dev
      imagePullPolicy: IfNotPresent
      job:
        args:
        - --kafka-bootstrap
        - host.docker.internal:9095
        - --clickhouse-url
        - http://host.docker.internal:8123/
        - --clickhouse-user
        - flinklab
        - --clickhouse-password
        - flinklab123
        - --group-id
        - p03-vehicle-alerts-k8s
        - --events-topic
        - vehicle.events
        - --alerts-topic
        - vehicle.alerts
        - --control-topic
        - vehicle.pattern.control
        entryClass: com.flywhl.flinklab.p03.VehicleAlertJob
        jarURI: local:///opt/flink/usrlib/p03-vehicle-monitoring.jar
        parallelism: 1
        state: running
        upgradeMode: savepoint
      jobManager:
        resource:
          cpu: 0.5
          memory: 1024m
      podTemplate:
        spec:
          containers:
          - env:
            - name: ENABLE_BUILT_IN_PLUGINS
              value: flink-s3-fs-hadoop-2.2.1.jar
            name: flink-main-container
      serviceAccount: flink
      taskManager:
        replicas: 1
        resource:
          cpu: 1
          memory: 2048m
status:
  abortTimestamp: "1970-01-01T00:00:00Z"
  blueGreenState: ACTIVE_BLUE
  deploymentReadyTimestamp: "1970-01-01T00:00:00Z"
  jobStatus:
    checkpointInfo:
      lastPeriodicCheckpointTimestamp: 0
    savepointInfo:
      lastPeriodicSavepointTimestamp: 0
      savepointHistory: []
    state: RUNNING
  lastReconciledSpec: '{"spec":{"ingress":null,"template":{"metadata":null,"spec":{"job":{"jarURI":"local:///opt/flink/usrlib/p03-vehicle-monitoring.jar","parallelism":1,"entryClass":"com.flywhl.flinklab.p03.VehicleAlertJob","args":["--kafka-bootstrap","host.docker.internal:9095","--clickhouse-url","http://host.docker.internal:8123/","--clickhouse-user","flinklab","--clickhouse-password","flinklab123","--group-id","p03-vehicle-alerts-k8s","--events-topic","vehicle.events","--alerts-topic","vehicle.alerts","--control-topic","vehicle.pattern.control"],"state":"running","savepointTriggerNonce":null,"initialSavepointPath":null,"checkpointTriggerNonce":null,"upgradeMode":"savepoint","allowNonRestoredState":null,"savepointRedeployNonce":null,"autoscalerResetNonce":null},"restartNonce":null,"flinkConfiguration":{"execution.checkpointing.dir":"s3://flink/checkpoints","execution.checkpointing.interval":"30s","execution.checkpointing.savepoint-dir":"s3://flink/savepoints","fs.s3a.access.key":"flinklab","fs.s3a.aws.credentials.provider":"org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider","fs.s3a.connection.ssl.enabled":"false","fs.s3a.endpoint":"http://host.docker.internal:9000","fs.s3a.path.style.access":"true","fs.s3a.secret.key":"flinklab123","s3.access-key":"flinklab","s3.endpoint":"http://host.docker.internal:9000","s3.path.style.access":"true","s3.secret-key":"flinklab123"},"image":"flinklab/p03-vehicle-alert:dev","imagePullPolicy":"IfNotPresent","serviceAccount":"flink","flinkVersion":"v2_2","ingress":null,"podTemplate":{"spec":{"containers":[{"env":[{"name":"ENABLE_BUILT_IN_PLUGINS","value":"flink-s3-fs-hadoop-2.2.1.jar"}],"name":"flink-main-container"}]}},"jobManager":{"resource":{"cpu":0.5,"memory":"1024m","ephemeralStorage":null},"replicas":1,"podTemplate":null},"taskManager":{"resource":{"cpu":1.0,"memory":"2048m","ephemeralStorage":null},"replicas":1,"podTemplate":null},"logConfiguration":null,"mode":null}},"configuration":{"kubernetes.operator.bluegreen.abort.grace-period":"8
    min","kubernetes.operator.bluegreen.deployment-deletion.delay":"0ms","kubernetes.operator.bluegreen.reconciliation.reschedule-interval":"15s"}}}'
  lastReconciledTimestamp: "2026-07-18T06:00:04.142629011Z"
```

### Child FlinkDeployments

```
NAME                        JOB STATUS   LIFECYCLE STATE
p03-vehicle-alert-bg-blue   RUNNING      STABLE
```

```yaml
apiVersion: v1
items:
- apiVersion: flink.apache.org/v1beta1
  kind: FlinkDeployment
  metadata:
    creationTimestamp: "2026-07-18T05:59:29Z"
    finalizers:
    - flinkdeployments.flink.apache.org/finalizer
    generation: 1
    labels:
      flink/blue-green-deployment-type: BLUE
    name: p03-vehicle-alert-bg-blue
    namespace: flink
    ownerReferences:
    - apiVersion: flink.apache.org/v1beta1
      blockOwnerDeletion: true
      controller: false
      kind: FlinkBlueGreenDeployment
      name: p03-vehicle-alert-bg
      uid: 7f19b8ab-0420-4706-8379-0be9b27126f1
    resourceVersion: "2654"
    uid: ef61eb4c-ede6-4a6b-a019-6d68f7cd48f3
  spec:
    flinkConfiguration:
      execution.checkpointing.dir: s3://flink/checkpoints
      execution.checkpointing.interval: 30s
      execution.checkpointing.savepoint-dir: s3://flink/savepoints
      fs.s3a.access.key: flinklab
      fs.s3a.aws.credentials.provider: org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
      fs.s3a.connection.ssl.enabled: "false"
      fs.s3a.endpoint: http://host.docker.internal:9000
      fs.s3a.path.style.access: "true"
      fs.s3a.secret.key: flinklab123
      s3.access-key: flinklab
      s3.endpoint: http://host.docker.internal:9000
      s3.path.style.access: "true"
      s3.secret-key: flinklab123
    flinkVersion: v2_2
    image: flinklab/p03-vehicle-alert:dev
    imagePullPolicy: IfNotPresent
    job:
      args:
      - --kafka-bootstrap
      - host.docker.internal:9095
      - --clickhouse-url
      - http://host.docker.internal:8123/
      - --clickhouse-user
      - flinklab
      - --clickhouse-password
      - flinklab123
      - --group-id
      - p03-vehicle-alerts-k8s
      - --events-topic
      - vehicle.events
      - --alerts-topic
      - vehicle.alerts
      - --control-topic
      - vehicle.pattern.control
      entryClass: com.flywhl.flinklab.p03.VehicleAlertJob
      jarURI: local:///opt/flink/usrlib/p03-vehicle-monitoring.jar
      parallelism: 1
      state: running
      upgradeMode: savepoint
    jobManager:
      replicas: 1
      resource:
        cpu: 0.5
        memory: 1024m
    podTemplate:
      spec:
        containers:
        - env:
          - name: ENABLE_BUILT_IN_PLUGINS
            value: flink-s3-fs-hadoop-2.2.1.jar
          name: flink-main-container
    serviceAccount: flink
    taskManager:
      replicas: 1
      resource:
        cpu: 1
        memory: 2048m
  status:
    clusterInfo:
      flink-revision: 450c63e @ 2026-05-03T10:10:44+02:00
      flink-version: 2.2.1
      state-size: "140770"
      total-cpu: "1.5"
      total-memory: "3221225472"
    conditions:
    - lastTransitionTime: "2026-07-18T06:00:04.149403376Z"
      message: Job status RUNNING
      reason: Running
      status: "True"
      type: Running
    jobManagerDeploymentStatus: READY
    jobStatus:
      checkpointInfo:
        lastPeriodicCheckpointTimestamp: 0
      jobId: 66f2185fc8ba08be40e161adeec74c37
      jobName: p03-vehicle-alert
      savepointInfo:
        lastPeriodicSavepointTimestamp: 0
        savepointHistory: []
      startTime: "1784354379909"
      state: RUNNING
      updateTime: "1784354404067"
    lifecycleState: STABLE
    observedGeneration: 1
    reconciliationStatus:
      lastReconciledSpec: '{"spec":{"job":{"jarURI":"local:///opt/flink/usrlib/p03-vehicle-monitoring.jar","parallelism":1,"entryClass":"com.flywhl.flinklab.p03.VehicleAlertJob","args":["--kafka-bootstrap","host.docker.internal:9095","--clickhouse-url","http://host.docker.internal:8123/","--clickhouse-user","flinklab","--clickhouse-password","flinklab123","--group-id","p03-vehicle-alerts-k8s","--events-topic","vehicle.events","--alerts-topic","vehicle.alerts","--control-topic","vehicle.pattern.control"],"state":"running","savepointTriggerNonce":null,"initialSavepointPath":null,"checkpointTriggerNonce":null,"upgradeMode":"stateless","allowNonRestoredState":null,"savepointRedeployNonce":null,"autoscalerResetNonce":null},"restartNonce":null,"flinkConfiguration":{"execution.checkpointing.dir":"s3://flink/checkpoints","execution.checkpointing.interval":"30s","execution.checkpointing.savepoint-dir":"s3://flink/savepoints","fs.s3a.access.key":"flinklab","fs.s3a.aws.credentials.provider":"org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider","fs.s3a.connection.ssl.enabled":"false","fs.s3a.endpoint":"http://host.docker.internal:9000","fs.s3a.path.style.access":"true","fs.s3a.secret.key":"flinklab123","s3.access-key":"flinklab","s3.endpoint":"http://host.docker.internal:9000","s3.path.style.access":"true","s3.secret-key":"flinklab123"},"image":"flinklab/p03-vehicle-alert:dev","imagePullPolicy":"IfNotPresent","serviceAccount":"flink","flinkVersion":"v2_2","ingress":null,"podTemplate":{"spec":{"containers":[{"env":[{"name":"ENABLE_BUILT_IN_PLUGINS","value":"flink-s3-fs-hadoop-2.2.1.jar"}],"name":"flink-main-container"}]}},"jobManager":{"resource":{"cpu":0.5,"memory":"1024m","ephemeralStorage":null},"replicas":1,"podTemplate":null},"taskManager":{"resource":{"cpu":1.0,"memory":"2048m","ephemeralStorage":null},"replicas":1,"podTemplate":null},"logConfiguration":null,"mode":null},"resource_metadata":{"apiVersion":"flink.apache.org/v1beta1","firstDeployment":true}}'
      lastStableSpec: '{"spec":{"job":{"jarURI":"local:///opt/flink/usrlib/p03-vehicle-monitoring.jar","parallelism":1,"entryClass":"com.flywhl.flinklab.p03.VehicleAlertJob","args":["--kafka-bootstrap","host.docker.internal:9095","--clickhouse-url","http://host.docker.internal:8123/","--clickhouse-user","flinklab","--clickhouse-password","flinklab123","--group-id","p03-vehicle-alerts-k8s","--events-topic","vehicle.events","--alerts-topic","vehicle.alerts","--control-topic","vehicle.pattern.control"],"state":"running","savepointTriggerNonce":null,"initialSavepointPath":null,"checkpointTriggerNonce":null,"upgradeMode":"stateless","allowNonRestoredState":null,"savepointRedeployNonce":null,"autoscalerResetNonce":null},"restartNonce":null,"flinkConfiguration":{"execution.checkpointing.dir":"s3://flink/checkpoints","execution.checkpointing.interval":"30s","execution.checkpointing.savepoint-dir":"s3://flink/savepoints","fs.s3a.access.key":"flinklab","fs.s3a.aws.credentials.provider":"org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider","fs.s3a.connection.ssl.enabled":"false","fs.s3a.endpoint":"http://host.docker.internal:9000","fs.s3a.path.style.access":"true","fs.s3a.secret.key":"flinklab123","s3.access-key":"flinklab","s3.endpoint":"http://host.docker.internal:9000","s3.path.style.access":"true","s3.secret-key":"flinklab123"},"image":"flinklab/p03-vehicle-alert:dev","imagePullPolicy":"IfNotPresent","serviceAccount":"flink","flinkVersion":"v2_2","ingress":null,"podTemplate":{"spec":{"containers":[{"env":[{"name":"ENABLE_BUILT_IN_PLUGINS","value":"flink-s3-fs-hadoop-2.2.1.jar"}],"name":"flink-main-container"}]}},"jobManager":{"resource":{"cpu":0.5,"memory":"1024m","ephemeralStorage":null},"replicas":1,"podTemplate":null},"taskManager":{"resource":{"cpu":1.0,"memory":"2048m","ephemeralStorage":null},"replicas":1,"podTemplate":null},"logConfiguration":null,"mode":null},"resource_metadata":{"apiVersion":"flink.apache.org/v1beta1","firstDeployment":true}}'
      reconciliationTimestamp: 1784354370115
      state: DEPLOYED
    taskManager:
      labelSelector: component=taskmanager,app=p03-vehicle-alert-bg-blue
      replicas: 1
kind: List
metadata:
  resourceVersion: ""
```

### Events

```
LAST SEEN   TYPE      REASON              OBJECT                                            MESSAGE
10m         Normal    Scheduled           pod/p03-vehicle-alert-bg-blue-taskmanager-1-1     Successfully assigned flink/p03-vehicle-alert-bg-blue-taskmanager-1-1 to orbstack
102s        Normal    Scheduled           pod/p03-vehicle-alert-bg-blue-taskmanager-1-1     Successfully assigned flink/p03-vehicle-alert-bg-blue-taskmanager-1-1 to orbstack
116s        Normal    Scheduled           pod/p03-vehicle-alert-bg-blue-868c48cf67-fhj4l    Successfully assigned flink/p03-vehicle-alert-bg-blue-868c48cf67-fhj4l to orbstack
10m         Normal    Scheduled           pod/p03-vehicle-alert-bg-blue-868c48cf67-sm79r    Successfully assigned flink/p03-vehicle-alert-bg-blue-868c48cf67-sm79r to orbstack
20m         Normal    Scheduled           pod/p03-vehicle-alert-bg-blue-868c48cf67-6qgrr    Successfully assigned flink/p03-vehicle-alert-bg-blue-868c48cf67-6qgrr to orbstack
20m         Normal    Submit              flinkdeployment/p03-vehicle-alert-bg-blue         Starting deployment
20m         Normal    SuccessfulCreate    replicaset/p03-vehicle-alert-bg-blue-868c48cf67   Created pod: p03-vehicle-alert-bg-blue-868c48cf67-6qgrr
20m         Normal    ScalingReplicaSet   deployment/p03-vehicle-alert-bg-blue              Scaled up replica set p03-vehicle-alert-bg-blue-868c48cf67 from 0 to 1
19m         Normal    JobStatusChanged    flinkdeployment/p03-vehicle-alert-bg-blue         Job status changed from RECONCILING to INITIALIZING
19m         Warning   Unhealthy           pod/p03-vehicle-alert-bg-blue-868c48cf67-6qgrr    Startup probe failed: Get "http://192.168.194.11:8081/config": dial tcp 192.168.194.11:8081: connect: connection refused
18m         Warning   CrashLoopBackOff    flinkdeployment/p03-vehicle-alert-bg-blue         [flink-main-container] back-off 20s restarting failed container=flink-main-container pod=p03-vehicle-alert-bg-blue-868c48cf67-6qgrr_flink(651bb1b3-de12-4121-837e-aedbd5ab4627)
18m         Warning   CrashLoopBackOff    flinkdeployment/p03-vehicle-alert-bg-blue         [flink-main-container] back-off 40s restarting failed container=flink-main-container pod=p03-vehicle-alert-bg-blue-868c48cf67-6qgrr_flink(651bb1b3-de12-4121-837e-aedbd5ab4627)
17m         Warning   CrashLoopBackOff    flinkdeployment/p03-vehicle-alert-bg-blue         [flink-main-container] back-off 1m20s restarting failed container=flink-main-container pod=p03-vehicle-alert-bg-blue-868c48cf67-6qgrr_flink(651bb1b3-de12-4121-837e-aedbd5ab4627)
15m         Warning   CrashLoopBackOff    flinkdeployment/p03-vehicle-alert-bg-blue         [flink-main-container] back-off 2m40s restarting failed container=flink-main-container pod=p03-vehicle-alert-bg-blue-868c48cf67-6qgrr_flink(651bb1b3-de12-4121-837e-aedbd5ab4627)
15m         Warning   BackOff             pod/p03-vehicle-alert-bg-blue-868c48cf67-6qgrr    Back-off restarting failed container flink-main-container in pod p03-vehicle-alert-bg-blue-868c48cf67-6qgrr_flink(651bb1b3-de12-4121-837e-aedbd5ab4627)
12m         Normal    Created             pod/p03-vehicle-alert-bg-blue-868c48cf67-6qgrr    Created container: flink-main-container
12m         Normal    Started             pod/p03-vehicle-alert-bg-blue-868c48cf67-6qgrr    Started container flink-main-container
12m         Normal    Pulled              pod/p03-vehicle-alert-bg-blue-868c48cf67-6qgrr    Container image "flinklab/p03-vehicle-alert:dev" already present on machine
12m         Warning   CrashLoopBackOff    flinkdeployment/p03-vehicle-alert-bg-blue         [flink-main-container] back-off 5m0s restarting failed container=flink-main-container pod=p03-vehicle-alert-bg-blue-868c48cf67-6qgrr_flink(651bb1b3-de12-4121-837e-aedbd5ab4627)
12m         Normal    SpecChanged         flinkdeployment/p03-vehicle-alert-bg-blue         UPGRADE change(s) detected (Diff: FlinkDeploymentSpec[job.state : running -> suspended, job.upgradeMode : stateless -> savepoint]), starting reconciliation.
11m         Warning   UpgradeFailed       flinkdeployment/p03-vehicle-alert-bg-blue         JobManager deployment is missing and HA metadata is not available to make stateful upgrades. It is possible that the job has finished or terminally failed, or the configmaps have been deleted.Manual restore required.
11m         Normal    Cleanup             flinkdeployment/p03-vehicle-alert-bg-blue         Cleaning up FlinkDeployment
11m         Normal    ScalingReplicaSet   deployment/p03-vehicle-alert-bg-blue              Scaled down replica set p03-vehicle-alert-bg-blue-868c48cf67 from 1 to 0
11m         Normal    SuccessfulDelete    replicaset/p03-vehicle-alert-bg-blue-868c48cf67   Deleted pod: p03-vehicle-alert-bg-blue-868c48cf67-6qgrr
10m         Normal    Submit              flinkdeployment/p03-vehicle-alert-bg-blue         Starting deployment
10m         Normal    ScalingReplicaSet   deployment/p03-vehicle-alert-bg-blue              Scaled up replica set p03-vehicle-alert-bg-blue-868c48cf67 from 0 to 1
10m         Normal    Started             pod/p03-vehicle-alert-bg-blue-868c48cf67-sm79r    Started container flink-main-container
10m         Normal    Created             pod/p03-vehicle-alert-bg-blue-868c48cf67-sm79r    Created container: flink-main-container
10m         Normal    Pulled              pod/p03-vehicle-alert-bg-blue-868c48cf67-sm79r    Container image "flinklab/p03-vehicle-alert:dev" already present on machine
10m         Normal    SuccessfulCreate    replicaset/p03-vehicle-alert-bg-blue-868c48cf67   Created pod: p03-vehicle-alert-bg-blue-868c48cf67-sm79r
10m         Warning   Unhealthy           pod/p03-vehicle-alert-bg-blue-868c48cf67-sm79r    Startup probe failed: Get "http://192.168.194.12:8081/config": dial tcp 192.168.194.12:8081: connect: connection refused
10m         Normal    Created             pod/p03-vehicle-alert-bg-blue-taskmanager-1-1     Created container: flink-main-container
10m         Normal    Pulled              pod/p03-vehicle-alert-bg-blue-taskmanager-1-1     Container image "flinklab/p03-vehicle-alert:dev" already present on machine
10m         Normal    Started             pod/p03-vehicle-alert-bg-blue-taskmanager-1-1     Started container flink-main-container
10m         Normal    JobStatusChanged    flinkdeployment/p03-vehicle-alert-bg-blue         Job status changed from RECONCILING to RESTARTING
5m30s       Warning   JobException        flinkdeployment/p03-vehicle-alert-bg-blue         org.apache.flink.util.FlinkException: Global failure triggered by OperatorCoordinator for 'Source: kafka-vehicle-events -> parse-vehicle-json -> Timestamps/Watermarks' (operator 46d1da3639fa719f605c845b064c5e2a)....
4m14s       Normal    Killing             pod/p03-vehicle-alert-bg-blue-taskmanager-1-1     Stopping container flink-main-container
3m30s       Warning   JobException        flinkdeployment/p03-vehicle-alert-bg-blue         org.apache.flink.util.FlinkException: Global failure triggered by OperatorCoordinator for 'Source: kafka-pattern-control -> parse-pattern-control' (operator 389491edbdab9d0ac4c2dbcd42257d61)....
2m44s       Normal    SpecChanged         flinkdeployment/p03-vehicle-alert-bg-blue         UPGRADE change(s) detected (Diff: FlinkDeploymentSpec[job.state : running -> suspended, job.upgradeMode : stateless -> savepoint]), starting reconciliation.
2m44s       Normal    Suspended           flinkdeployment/p03-vehicle-alert-bg-blue         Suspending existing deployment.
2m39s       Normal    JobStatusChanged    flinkdeployment/p03-vehicle-alert-bg-blue         Job status changed from CANCELLING to CANCELED
2m3s        Normal    SuccessfulDelete    replicaset/p03-vehicle-alert-bg-blue-868c48cf67   Deleted pod: p03-vehicle-alert-bg-blue-868c48cf67-sm79r
2m3s        Normal    Cleanup             flinkdeployment/p03-vehicle-alert-bg-blue         Cleaning up FlinkDeployment
2m3s        Normal    Killing             pod/p03-vehicle-alert-bg-blue-868c48cf67-sm79r    Stopping container flink-main-container
2m3s        Normal    ScalingReplicaSet   deployment/p03-vehicle-alert-bg-blue              Scaled down replica set p03-vehicle-alert-bg-blue-868c48cf67 from 1 to 0
117s        Normal    Submit              flinkdeployment/p03-vehicle-alert-bg-blue         Starting deployment
116s        Normal    Started             pod/p03-vehicle-alert-bg-blue-868c48cf67-fhj4l    Started container flink-main-container
116s        Normal    Pulled              pod/p03-vehicle-alert-bg-blue-868c48cf67-fhj4l    Container image "flinklab/p03-vehicle-alert:dev" already present on machine
116s        Normal    Created             pod/p03-vehicle-alert-bg-blue-868c48cf67-fhj4l    Created container: flink-main-container
116s        Normal    SuccessfulCreate    replicaset/p03-vehicle-alert-bg-blue-868c48cf67   Created pod: p03-vehicle-alert-bg-blue-868c48cf67-fhj4l
116s        Normal    ScalingReplicaSet   deployment/p03-vehicle-alert-bg-blue              Scaled up replica set p03-vehicle-alert-bg-blue-868c48cf67 from 0 to 1
109s        Warning   Unhealthy           pod/p03-vehicle-alert-bg-blue-868c48cf67-fhj4l    Startup probe failed: Get "http://192.168.194.16:8081/config": dial tcp 192.168.194.16:8081: connect: connection refused
102s        Normal    Pulled              pod/p03-vehicle-alert-bg-blue-taskmanager-1-1     Container image "flinklab/p03-vehicle-alert:dev" already present on machine
102s        Normal    Created             pod/p03-vehicle-alert-bg-blue-taskmanager-1-1     Created container: flink-main-container
102s        Normal    Started             pod/p03-vehicle-alert-bg-blue-taskmanager-1-1     Started container flink-main-container
98s         Normal    JobStatusChanged    flinkdeployment/p03-vehicle-alert-bg-blue         Job status changed from RECONCILING to CREATED
82s         Normal    JobStatusChanged    flinkdeployment/p03-vehicle-alert-bg-blue         Job status changed from CREATED to RUNNING
```

## Trigger TRANSITION

UTC: 2026-07-18T06:01:26Z

- From: `ACTIVE_BLUE`
- To (expected): `ACTIVE_GREEN`
- Command: `helm upgrade --install p03-vehicle-alert ... --set image.tag=dev-green`


### Snapshot during SAVEPOINTING_BLUE

UTC: 2026-07-18T06:01:26Z

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkBlueGreenDeployment
metadata:
  annotations:
    meta.helm.sh/release-name: p03-vehicle-alert
    meta.helm.sh/release-namespace: flink
  creationTimestamp: "2026-07-18T05:59:29Z"
  generation: 2
  labels:
    app.kubernetes.io/managed-by: Helm
    app.kubernetes.io/name: p03-vehicle-alert
    app.kubernetes.io/part-of: flink-learning-lab
  name: p03-vehicle-alert-bg
  namespace: flink
  resourceVersion: "2731"
  uid: 7f19b8ab-0420-4706-8379-0be9b27126f1
spec:
  configuration:
    kubernetes.operator.bluegreen.abort.grace-period: 8 min
    kubernetes.operator.bluegreen.deployment-deletion.delay: 0ms
    kubernetes.operator.bluegreen.reconciliation.reschedule-interval: 15s
  template:
    spec:
      flinkConfiguration:
        execution.checkpointing.dir: s3://flink/checkpoints
        execution.checkpointing.interval: 30s
        execution.checkpointing.savepoint-dir: s3://flink/savepoints
        fs.s3a.access.key: flinklab
        fs.s3a.aws.credentials.provider: org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
        fs.s3a.connection.ssl.enabled: "false"
        fs.s3a.endpoint: http://host.docker.internal:9000
        fs.s3a.path.style.access: "true"
        fs.s3a.secret.key: flinklab123
        s3.access-key: flinklab
        s3.endpoint: http://host.docker.internal:9000
        s3.path.style.access: "true"
        s3.secret-key: flinklab123
      flinkVersion: v2_2
      image: flinklab/p03-vehicle-alert:dev-green
      imagePullPolicy: IfNotPresent
      job:
        args:
        - --kafka-bootstrap
        - host.docker.internal:9095
        - --clickhouse-url
        - http://host.docker.internal:8123/
        - --clickhouse-user
        - flinklab
        - --clickhouse-password
        - flinklab123
        - --group-id
        - p03-vehicle-alerts-k8s
        - --events-topic
        - vehicle.events
        - --alerts-topic
        - vehicle.alerts
        - --control-topic
        - vehicle.pattern.control
        entryClass: com.flywhl.flinklab.p03.VehicleAlertJob
        jarURI: local:///opt/flink/usrlib/p03-vehicle-monitoring.jar
        parallelism: 1
        state: running
        upgradeMode: savepoint
      jobManager:
        resource:
          cpu: 0.5
          memory: 1024m
      podTemplate:
        spec:
          containers:
          - env:
            - name: ENABLE_BUILT_IN_PLUGINS
              value: flink-s3-fs-hadoop-2.2.1.jar
            name: flink-main-container
      serviceAccount: flink
      taskManager:
        replicas: 1
        resource:
          cpu: 1
          memory: 2048m
status:
  abortTimestamp: "1970-01-01T00:00:00Z"
  blueGreenState: SAVEPOINTING_BLUE
  deploymentReadyTimestamp: "1970-01-01T00:00:00Z"
  jobStatus:
    checkpointInfo:
      lastPeriodicCheckpointTimestamp: 0
    savepointInfo:
      lastPeriodicSavepointTimestamp: 0
      savepointHistory: []
    state: RUNNING
  lastReconciledSpec: '{"spec":{"ingress":null,"template":{"metadata":null,"spec":{"job":{"jarURI":"local:///opt/flink/usrlib/p03-vehicle-monitoring.jar","parallelism":1,"entryClass":"com.flywhl.flinklab.p03.VehicleAlertJob","args":["--kafka-bootstrap","host.docker.internal:9095","--clickhouse-url","http://host.docker.internal:8123/","--clickhouse-user","flinklab","--clickhouse-password","flinklab123","--group-id","p03-vehicle-alerts-k8s","--events-topic","vehicle.events","--alerts-topic","vehicle.alerts","--control-topic","vehicle.pattern.control"],"state":"running","savepointTriggerNonce":null,"initialSavepointPath":null,"checkpointTriggerNonce":null,"upgradeMode":"savepoint","allowNonRestoredState":null,"savepointRedeployNonce":null,"autoscalerResetNonce":null},"restartNonce":null,"flinkConfiguration":{"execution.checkpointing.dir":"s3://flink/checkpoints","execution.checkpointing.interval":"30s","execution.checkpointing.savepoint-dir":"s3://flink/savepoints","fs.s3a.access.key":"flinklab","fs.s3a.aws.credentials.provider":"org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider","fs.s3a.connection.ssl.enabled":"false","fs.s3a.endpoint":"http://host.docker.internal:9000","fs.s3a.path.style.access":"true","fs.s3a.secret.key":"flinklab123","s3.access-key":"flinklab","s3.endpoint":"http://host.docker.internal:9000","s3.path.style.access":"true","s3.secret-key":"flinklab123"},"image":"flinklab/p03-vehicle-alert:dev","imagePullPolicy":"IfNotPresent","serviceAccount":"flink","flinkVersion":"v2_2","ingress":null,"podTemplate":{"spec":{"containers":[{"env":[{"name":"ENABLE_BUILT_IN_PLUGINS","value":"flink-s3-fs-hadoop-2.2.1.jar"}],"name":"flink-main-container"}]}},"jobManager":{"resource":{"cpu":0.5,"memory":"1024m","ephemeralStorage":null},"replicas":1,"podTemplate":null},"taskManager":{"resource":{"cpu":1.0,"memory":"2048m","ephemeralStorage":null},"replicas":1,"podTemplate":null},"logConfiguration":null,"mode":null}},"configuration":{"kubernetes.operator.bluegreen.abort.grace-period":"8
    min","kubernetes.operator.bluegreen.deployment-deletion.delay":"0ms","kubernetes.operator.bluegreen.reconciliation.reschedule-interval":"15s"}}}'
  lastReconciledTimestamp: "2026-07-18T06:01:26.870512295Z"
  savepointTriggerId: f98c4b73a3e509b565f01db5da3ad313
```

```
NAME                        JOB STATUS   LIFECYCLE STATE
p03-vehicle-alert-bg-blue   RUNNING      STABLE
```

### Snapshot during SAVEPOINTING_BLUE

UTC: 2026-07-18T06:01:37Z

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkBlueGreenDeployment
metadata:
  annotations:
    meta.helm.sh/release-name: p03-vehicle-alert
    meta.helm.sh/release-namespace: flink
  creationTimestamp: "2026-07-18T05:59:29Z"
  generation: 2
  labels:
    app.kubernetes.io/managed-by: Helm
    app.kubernetes.io/name: p03-vehicle-alert
    app.kubernetes.io/part-of: flink-learning-lab
  name: p03-vehicle-alert-bg
  namespace: flink
  resourceVersion: "2731"
  uid: 7f19b8ab-0420-4706-8379-0be9b27126f1
spec:
  configuration:
    kubernetes.operator.bluegreen.abort.grace-period: 8 min
    kubernetes.operator.bluegreen.deployment-deletion.delay: 0ms
    kubernetes.operator.bluegreen.reconciliation.reschedule-interval: 15s
  template:
    spec:
      flinkConfiguration:
        execution.checkpointing.dir: s3://flink/checkpoints
        execution.checkpointing.interval: 30s
        execution.checkpointing.savepoint-dir: s3://flink/savepoints
        fs.s3a.access.key: flinklab
        fs.s3a.aws.credentials.provider: org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
        fs.s3a.connection.ssl.enabled: "false"
        fs.s3a.endpoint: http://host.docker.internal:9000
        fs.s3a.path.style.access: "true"
        fs.s3a.secret.key: flinklab123
        s3.access-key: flinklab
        s3.endpoint: http://host.docker.internal:9000
        s3.path.style.access: "true"
        s3.secret-key: flinklab123
      flinkVersion: v2_2
      image: flinklab/p03-vehicle-alert:dev-green
      imagePullPolicy: IfNotPresent
      job:
        args:
        - --kafka-bootstrap
        - host.docker.internal:9095
        - --clickhouse-url
        - http://host.docker.internal:8123/
        - --clickhouse-user
        - flinklab
        - --clickhouse-password
        - flinklab123
        - --group-id
        - p03-vehicle-alerts-k8s
        - --events-topic
        - vehicle.events
        - --alerts-topic
        - vehicle.alerts
        - --control-topic
        - vehicle.pattern.control
        entryClass: com.flywhl.flinklab.p03.VehicleAlertJob
        jarURI: local:///opt/flink/usrlib/p03-vehicle-monitoring.jar
        parallelism: 1
        state: running
        upgradeMode: savepoint
      jobManager:
        resource:
          cpu: 0.5
          memory: 1024m
      podTemplate:
        spec:
          containers:
          - env:
            - name: ENABLE_BUILT_IN_PLUGINS
              value: flink-s3-fs-hadoop-2.2.1.jar
            name: flink-main-container
      serviceAccount: flink
      taskManager:
        replicas: 1
        resource:
          cpu: 1
          memory: 2048m
status:
  abortTimestamp: "1970-01-01T00:00:00Z"
  blueGreenState: SAVEPOINTING_BLUE
  deploymentReadyTimestamp: "1970-01-01T00:00:00Z"
  jobStatus:
    checkpointInfo:
      lastPeriodicCheckpointTimestamp: 0
    savepointInfo:
      lastPeriodicSavepointTimestamp: 0
      savepointHistory: []
    state: RUNNING
  lastReconciledSpec: '{"spec":{"ingress":null,"template":{"metadata":null,"spec":{"job":{"jarURI":"local:///opt/flink/usrlib/p03-vehicle-monitoring.jar","parallelism":1,"entryClass":"com.flywhl.flinklab.p03.VehicleAlertJob","args":["--kafka-bootstrap","host.docker.internal:9095","--clickhouse-url","http://host.docker.internal:8123/","--clickhouse-user","flinklab","--clickhouse-password","flinklab123","--group-id","p03-vehicle-alerts-k8s","--events-topic","vehicle.events","--alerts-topic","vehicle.alerts","--control-topic","vehicle.pattern.control"],"state":"running","savepointTriggerNonce":null,"initialSavepointPath":null,"checkpointTriggerNonce":null,"upgradeMode":"savepoint","allowNonRestoredState":null,"savepointRedeployNonce":null,"autoscalerResetNonce":null},"restartNonce":null,"flinkConfiguration":{"execution.checkpointing.dir":"s3://flink/checkpoints","execution.checkpointing.interval":"30s","execution.checkpointing.savepoint-dir":"s3://flink/savepoints","fs.s3a.access.key":"flinklab","fs.s3a.aws.credentials.provider":"org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider","fs.s3a.connection.ssl.enabled":"false","fs.s3a.endpoint":"http://host.docker.internal:9000","fs.s3a.path.style.access":"true","fs.s3a.secret.key":"flinklab123","s3.access-key":"flinklab","s3.endpoint":"http://host.docker.internal:9000","s3.path.style.access":"true","s3.secret-key":"flinklab123"},"image":"flinklab/p03-vehicle-alert:dev","imagePullPolicy":"IfNotPresent","serviceAccount":"flink","flinkVersion":"v2_2","ingress":null,"podTemplate":{"spec":{"containers":[{"env":[{"name":"ENABLE_BUILT_IN_PLUGINS","value":"flink-s3-fs-hadoop-2.2.1.jar"}],"name":"flink-main-container"}]}},"jobManager":{"resource":{"cpu":0.5,"memory":"1024m","ephemeralStorage":null},"replicas":1,"podTemplate":null},"taskManager":{"resource":{"cpu":1.0,"memory":"2048m","ephemeralStorage":null},"replicas":1,"podTemplate":null},"logConfiguration":null,"mode":null}},"configuration":{"kubernetes.operator.bluegreen.abort.grace-period":"8
    min","kubernetes.operator.bluegreen.deployment-deletion.delay":"0ms","kubernetes.operator.bluegreen.reconciliation.reschedule-interval":"15s"}}}'
  lastReconciledTimestamp: "2026-07-18T06:01:26.870512295Z"
  savepointTriggerId: f98c4b73a3e509b565f01db5da3ad313
```

```
NAME                        JOB STATUS   LIFECYCLE STATE
p03-vehicle-alert-bg-blue   RUNNING      STABLE
```

### Snapshot during TRANSITIONING_TO_GREEN

UTC: 2026-07-18T06:01:47Z

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkBlueGreenDeployment
metadata:
  annotations:
    meta.helm.sh/release-name: p03-vehicle-alert
    meta.helm.sh/release-namespace: flink
  creationTimestamp: "2026-07-18T05:59:29Z"
  generation: 2
  labels:
    app.kubernetes.io/managed-by: Helm
    app.kubernetes.io/name: p03-vehicle-alert
    app.kubernetes.io/part-of: flink-learning-lab
  name: p03-vehicle-alert-bg
  namespace: flink
  resourceVersion: "2769"
  uid: 7f19b8ab-0420-4706-8379-0be9b27126f1
spec:
  configuration:
    kubernetes.operator.bluegreen.abort.grace-period: 8 min
    kubernetes.operator.bluegreen.deployment-deletion.delay: 0ms
    kubernetes.operator.bluegreen.reconciliation.reschedule-interval: 15s
  template:
    spec:
      flinkConfiguration:
        execution.checkpointing.dir: s3://flink/checkpoints
        execution.checkpointing.interval: 30s
        execution.checkpointing.savepoint-dir: s3://flink/savepoints
        fs.s3a.access.key: flinklab
        fs.s3a.aws.credentials.provider: org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
        fs.s3a.connection.ssl.enabled: "false"
        fs.s3a.endpoint: http://host.docker.internal:9000
        fs.s3a.path.style.access: "true"
        fs.s3a.secret.key: flinklab123
        s3.access-key: flinklab
        s3.endpoint: http://host.docker.internal:9000
        s3.path.style.access: "true"
        s3.secret-key: flinklab123
      flinkVersion: v2_2
      image: flinklab/p03-vehicle-alert:dev-green
      imagePullPolicy: IfNotPresent
      job:
        args:
        - --kafka-bootstrap
        - host.docker.internal:9095
        - --clickhouse-url
        - http://host.docker.internal:8123/
        - --clickhouse-user
        - flinklab
        - --clickhouse-password
        - flinklab123
        - --group-id
        - p03-vehicle-alerts-k8s
        - --events-topic
        - vehicle.events
        - --alerts-topic
        - vehicle.alerts
        - --control-topic
        - vehicle.pattern.control
        entryClass: com.flywhl.flinklab.p03.VehicleAlertJob
        jarURI: local:///opt/flink/usrlib/p03-vehicle-monitoring.jar
        parallelism: 1
        state: running
        upgradeMode: savepoint
      jobManager:
        resource:
          cpu: 0.5
          memory: 1024m
      podTemplate:
        spec:
          containers:
          - env:
            - name: ENABLE_BUILT_IN_PLUGINS
              value: flink-s3-fs-hadoop-2.2.1.jar
            name: flink-main-container
      serviceAccount: flink
      taskManager:
        replicas: 1
        resource:
          cpu: 1
          memory: 2048m
status:
  abortTimestamp: "2026-07-18T06:09:41.935Z"
  blueGreenState: TRANSITIONING_TO_GREEN
  deploymentReadyTimestamp: "1970-01-01T00:00:00Z"
  jobStatus:
    checkpointInfo:
      lastPeriodicCheckpointTimestamp: 0
    savepointInfo:
      lastPeriodicSavepointTimestamp: 0
      savepointHistory: []
    state: RECONCILING
  lastReconciledSpec: '{"spec":{"ingress":null,"template":{"metadata":null,"spec":{"job":{"jarURI":"local:///opt/flink/usrlib/p03-vehicle-monitoring.jar","parallelism":1,"entryClass":"com.flywhl.flinklab.p03.VehicleAlertJob","args":["--kafka-bootstrap","host.docker.internal:9095","--clickhouse-url","http://host.docker.internal:8123/","--clickhouse-user","flinklab","--clickhouse-password","flinklab123","--group-id","p03-vehicle-alerts-k8s","--events-topic","vehicle.events","--alerts-topic","vehicle.alerts","--control-topic","vehicle.pattern.control"],"state":"running","savepointTriggerNonce":null,"initialSavepointPath":null,"checkpointTriggerNonce":null,"upgradeMode":"savepoint","allowNonRestoredState":null,"savepointRedeployNonce":null,"autoscalerResetNonce":null},"restartNonce":null,"flinkConfiguration":{"execution.checkpointing.dir":"s3://flink/checkpoints","execution.checkpointing.interval":"30s","execution.checkpointing.savepoint-dir":"s3://flink/savepoints","fs.s3a.access.key":"flinklab","fs.s3a.aws.credentials.provider":"org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider","fs.s3a.connection.ssl.enabled":"false","fs.s3a.endpoint":"http://host.docker.internal:9000","fs.s3a.path.style.access":"true","fs.s3a.secret.key":"flinklab123","s3.access-key":"flinklab","s3.endpoint":"http://host.docker.internal:9000","s3.path.style.access":"true","s3.secret-key":"flinklab123"},"image":"flinklab/p03-vehicle-alert:dev-green","imagePullPolicy":"IfNotPresent","serviceAccount":"flink","flinkVersion":"v2_2","ingress":null,"podTemplate":{"spec":{"containers":[{"env":[{"name":"ENABLE_BUILT_IN_PLUGINS","value":"flink-s3-fs-hadoop-2.2.1.jar"}],"name":"flink-main-container"}]}},"jobManager":{"resource":{"cpu":0.5,"memory":"1024m","ephemeralStorage":null},"replicas":1,"podTemplate":null},"taskManager":{"resource":{"cpu":1.0,"memory":"2048m","ephemeralStorage":null},"replicas":1,"podTemplate":null},"logConfiguration":null,"mode":null}},"configuration":{"kubernetes.operator.bluegreen.abort.grace-period":"8
    min","kubernetes.operator.bluegreen.deployment-deletion.delay":"0ms","kubernetes.operator.bluegreen.reconciliation.reschedule-interval":"15s"}}}'
  lastReconciledTimestamp: "2026-07-18T06:01:42.156592333Z"
  savepointTriggerId: f98c4b73a3e509b565f01db5da3ad313
```

```
NAME                         JOB STATUS    LIFECYCLE STATE
p03-vehicle-alert-bg-blue    RUNNING       STABLE
p03-vehicle-alert-bg-green   RECONCILING   DEPLOYED
```

### Snapshot during TRANSITIONING_TO_GREEN

UTC: 2026-07-18T06:01:58Z

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkBlueGreenDeployment
metadata:
  annotations:
    meta.helm.sh/release-name: p03-vehicle-alert
    meta.helm.sh/release-namespace: flink
  creationTimestamp: "2026-07-18T05:59:29Z"
  generation: 2
  labels:
    app.kubernetes.io/managed-by: Helm
    app.kubernetes.io/name: p03-vehicle-alert
    app.kubernetes.io/part-of: flink-learning-lab
  name: p03-vehicle-alert-bg
  namespace: flink
  resourceVersion: "2799"
  uid: 7f19b8ab-0420-4706-8379-0be9b27126f1
spec:
  configuration:
    kubernetes.operator.bluegreen.abort.grace-period: 8 min
    kubernetes.operator.bluegreen.deployment-deletion.delay: 0ms
    kubernetes.operator.bluegreen.reconciliation.reschedule-interval: 15s
  template:
    spec:
      flinkConfiguration:
        execution.checkpointing.dir: s3://flink/checkpoints
        execution.checkpointing.interval: 30s
        execution.checkpointing.savepoint-dir: s3://flink/savepoints
        fs.s3a.access.key: flinklab
        fs.s3a.aws.credentials.provider: org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
        fs.s3a.connection.ssl.enabled: "false"
        fs.s3a.endpoint: http://host.docker.internal:9000
        fs.s3a.path.style.access: "true"
        fs.s3a.secret.key: flinklab123
        s3.access-key: flinklab
        s3.endpoint: http://host.docker.internal:9000
        s3.path.style.access: "true"
        s3.secret-key: flinklab123
      flinkVersion: v2_2
      image: flinklab/p03-vehicle-alert:dev-green
      imagePullPolicy: IfNotPresent
      job:
        args:
        - --kafka-bootstrap
        - host.docker.internal:9095
        - --clickhouse-url
        - http://host.docker.internal:8123/
        - --clickhouse-user
        - flinklab
        - --clickhouse-password
        - flinklab123
        - --group-id
        - p03-vehicle-alerts-k8s
        - --events-topic
        - vehicle.events
        - --alerts-topic
        - vehicle.alerts
        - --control-topic
        - vehicle.pattern.control
        entryClass: com.flywhl.flinklab.p03.VehicleAlertJob
        jarURI: local:///opt/flink/usrlib/p03-vehicle-monitoring.jar
        parallelism: 1
        state: running
        upgradeMode: savepoint
      jobManager:
        resource:
          cpu: 0.5
          memory: 1024m
      podTemplate:
        spec:
          containers:
          - env:
            - name: ENABLE_BUILT_IN_PLUGINS
              value: flink-s3-fs-hadoop-2.2.1.jar
            name: flink-main-container
      serviceAccount: flink
      taskManager:
        replicas: 1
        resource:
          cpu: 1
          memory: 2048m
status:
  abortTimestamp: "2026-07-18T06:09:41.935Z"
  blueGreenState: TRANSITIONING_TO_GREEN
  deploymentReadyTimestamp: "1970-01-01T00:00:00Z"
  jobStatus:
    checkpointInfo:
      lastPeriodicCheckpointTimestamp: 0
    savepointInfo:
      lastPeriodicSavepointTimestamp: 0
      savepointHistory: []
    state: RECONCILING
  lastReconciledSpec: '{"spec":{"ingress":null,"template":{"metadata":null,"spec":{"job":{"jarURI":"local:///opt/flink/usrlib/p03-vehicle-monitoring.jar","parallelism":1,"entryClass":"com.flywhl.flinklab.p03.VehicleAlertJob","args":["--kafka-bootstrap","host.docker.internal:9095","--clickhouse-url","http://host.docker.internal:8123/","--clickhouse-user","flinklab","--clickhouse-password","flinklab123","--group-id","p03-vehicle-alerts-k8s","--events-topic","vehicle.events","--alerts-topic","vehicle.alerts","--control-topic","vehicle.pattern.control"],"state":"running","savepointTriggerNonce":null,"initialSavepointPath":null,"checkpointTriggerNonce":null,"upgradeMode":"savepoint","allowNonRestoredState":null,"savepointRedeployNonce":null,"autoscalerResetNonce":null},"restartNonce":null,"flinkConfiguration":{"execution.checkpointing.dir":"s3://flink/checkpoints","execution.checkpointing.interval":"30s","execution.checkpointing.savepoint-dir":"s3://flink/savepoints","fs.s3a.access.key":"flinklab","fs.s3a.aws.credentials.provider":"org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider","fs.s3a.connection.ssl.enabled":"false","fs.s3a.endpoint":"http://host.docker.internal:9000","fs.s3a.path.style.access":"true","fs.s3a.secret.key":"flinklab123","s3.access-key":"flinklab","s3.endpoint":"http://host.docker.internal:9000","s3.path.style.access":"true","s3.secret-key":"flinklab123"},"image":"flinklab/p03-vehicle-alert:dev-green","imagePullPolicy":"IfNotPresent","serviceAccount":"flink","flinkVersion":"v2_2","ingress":null,"podTemplate":{"spec":{"containers":[{"env":[{"name":"ENABLE_BUILT_IN_PLUGINS","value":"flink-s3-fs-hadoop-2.2.1.jar"}],"name":"flink-main-container"}]}},"jobManager":{"resource":{"cpu":0.5,"memory":"1024m","ephemeralStorage":null},"replicas":1,"podTemplate":null},"taskManager":{"resource":{"cpu":1.0,"memory":"2048m","ephemeralStorage":null},"replicas":1,"podTemplate":null},"logConfiguration":null,"mode":null}},"configuration":{"kubernetes.operator.bluegreen.abort.grace-period":"8
    min","kubernetes.operator.bluegreen.deployment-deletion.delay":"0ms","kubernetes.operator.bluegreen.reconciliation.reschedule-interval":"15s"}}}'
  lastReconciledTimestamp: "2026-07-18T06:01:50.972977967Z"
  savepointTriggerId: f98c4b73a3e509b565f01db5da3ad313
```

```
NAME                         JOB STATUS    LIFECYCLE STATE
p03-vehicle-alert-bg-blue    RUNNING       STABLE
p03-vehicle-alert-bg-green   RECONCILING   DEPLOYED
```

### Snapshot during TRANSITIONING_TO_GREEN

UTC: 2026-07-18T06:02:08Z

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkBlueGreenDeployment
metadata:
  annotations:
    meta.helm.sh/release-name: p03-vehicle-alert
    meta.helm.sh/release-namespace: flink
  creationTimestamp: "2026-07-18T05:59:29Z"
  generation: 2
  labels:
    app.kubernetes.io/managed-by: Helm
    app.kubernetes.io/name: p03-vehicle-alert
    app.kubernetes.io/part-of: flink-learning-lab
  name: p03-vehicle-alert-bg
  namespace: flink
  resourceVersion: "2818"
  uid: 7f19b8ab-0420-4706-8379-0be9b27126f1
spec:
  configuration:
    kubernetes.operator.bluegreen.abort.grace-period: 8 min
    kubernetes.operator.bluegreen.deployment-deletion.delay: 0ms
    kubernetes.operator.bluegreen.reconciliation.reschedule-interval: 15s
  template:
    spec:
      flinkConfiguration:
        execution.checkpointing.dir: s3://flink/checkpoints
        execution.checkpointing.interval: 30s
        execution.checkpointing.savepoint-dir: s3://flink/savepoints
        fs.s3a.access.key: flinklab
        fs.s3a.aws.credentials.provider: org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
        fs.s3a.connection.ssl.enabled: "false"
        fs.s3a.endpoint: http://host.docker.internal:9000
        fs.s3a.path.style.access: "true"
        fs.s3a.secret.key: flinklab123
        s3.access-key: flinklab
        s3.endpoint: http://host.docker.internal:9000
        s3.path.style.access: "true"
        s3.secret-key: flinklab123
      flinkVersion: v2_2
      image: flinklab/p03-vehicle-alert:dev-green
      imagePullPolicy: IfNotPresent
      job:
        args:
        - --kafka-bootstrap
        - host.docker.internal:9095
        - --clickhouse-url
        - http://host.docker.internal:8123/
        - --clickhouse-user
        - flinklab
        - --clickhouse-password
        - flinklab123
        - --group-id
        - p03-vehicle-alerts-k8s
        - --events-topic
        - vehicle.events
        - --alerts-topic
        - vehicle.alerts
        - --control-topic
        - vehicle.pattern.control
        entryClass: com.flywhl.flinklab.p03.VehicleAlertJob
        jarURI: local:///opt/flink/usrlib/p03-vehicle-monitoring.jar
        parallelism: 1
        state: running
        upgradeMode: savepoint
      jobManager:
        resource:
          cpu: 0.5
          memory: 1024m
      podTemplate:
        spec:
          containers:
          - env:
            - name: ENABLE_BUILT_IN_PLUGINS
              value: flink-s3-fs-hadoop-2.2.1.jar
            name: flink-main-container
      serviceAccount: flink
      taskManager:
        replicas: 1
        resource:
          cpu: 1
          memory: 2048m
status:
  abortTimestamp: "2026-07-18T06:09:41.935Z"
  blueGreenState: TRANSITIONING_TO_GREEN
  deploymentReadyTimestamp: "1970-01-01T00:00:00Z"
  jobStatus:
    checkpointInfo:
      lastPeriodicCheckpointTimestamp: 0
    savepointInfo:
      lastPeriodicSavepointTimestamp: 0
      savepointHistory: []
    state: RECONCILING
  lastReconciledSpec: '{"spec":{"ingress":null,"template":{"metadata":null,"spec":{"job":{"jarURI":"local:///opt/flink/usrlib/p03-vehicle-monitoring.jar","parallelism":1,"entryClass":"com.flywhl.flinklab.p03.VehicleAlertJob","args":["--kafka-bootstrap","host.docker.internal:9095","--clickhouse-url","http://host.docker.internal:8123/","--clickhouse-user","flinklab","--clickhouse-password","flinklab123","--group-id","p03-vehicle-alerts-k8s","--events-topic","vehicle.events","--alerts-topic","vehicle.alerts","--control-topic","vehicle.pattern.control"],"state":"running","savepointTriggerNonce":null,"initialSavepointPath":null,"checkpointTriggerNonce":null,"upgradeMode":"savepoint","allowNonRestoredState":null,"savepointRedeployNonce":null,"autoscalerResetNonce":null},"restartNonce":null,"flinkConfiguration":{"execution.checkpointing.dir":"s3://flink/checkpoints","execution.checkpointing.interval":"30s","execution.checkpointing.savepoint-dir":"s3://flink/savepoints","fs.s3a.access.key":"flinklab","fs.s3a.aws.credentials.provider":"org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider","fs.s3a.connection.ssl.enabled":"false","fs.s3a.endpoint":"http://host.docker.internal:9000","fs.s3a.path.style.access":"true","fs.s3a.secret.key":"flinklab123","s3.access-key":"flinklab","s3.endpoint":"http://host.docker.internal:9000","s3.path.style.access":"true","s3.secret-key":"flinklab123"},"image":"flinklab/p03-vehicle-alert:dev-green","imagePullPolicy":"IfNotPresent","serviceAccount":"flink","flinkVersion":"v2_2","ingress":null,"podTemplate":{"spec":{"containers":[{"env":[{"name":"ENABLE_BUILT_IN_PLUGINS","value":"flink-s3-fs-hadoop-2.2.1.jar"}],"name":"flink-main-container"}]}},"jobManager":{"resource":{"cpu":0.5,"memory":"1024m","ephemeralStorage":null},"replicas":1,"podTemplate":null},"taskManager":{"resource":{"cpu":1.0,"memory":"2048m","ephemeralStorage":null},"replicas":1,"podTemplate":null},"logConfiguration":null,"mode":null}},"configuration":{"kubernetes.operator.bluegreen.abort.grace-period":"8
    min","kubernetes.operator.bluegreen.deployment-deletion.delay":"0ms","kubernetes.operator.bluegreen.reconciliation.reschedule-interval":"15s"}}}'
  lastReconciledTimestamp: "2026-07-18T06:02:04.446674221Z"
  savepointTriggerId: f98c4b73a3e509b565f01db5da3ad313
```

```
NAME                         JOB STATUS   LIFECYCLE STATE
p03-vehicle-alert-bg-blue    RUNNING      STABLE
p03-vehicle-alert-bg-green   CREATED      DEPLOYED
```

### Snapshot during TRANSITIONING_TO_GREEN

UTC: 2026-07-18T06:02:18Z

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkBlueGreenDeployment
metadata:
  annotations:
    meta.helm.sh/release-name: p03-vehicle-alert
    meta.helm.sh/release-namespace: flink
  creationTimestamp: "2026-07-18T05:59:29Z"
  generation: 2
  labels:
    app.kubernetes.io/managed-by: Helm
    app.kubernetes.io/name: p03-vehicle-alert
    app.kubernetes.io/part-of: flink-learning-lab
  name: p03-vehicle-alert-bg
  namespace: flink
  resourceVersion: "2824"
  uid: 7f19b8ab-0420-4706-8379-0be9b27126f1
spec:
  configuration:
    kubernetes.operator.bluegreen.abort.grace-period: 8 min
    kubernetes.operator.bluegreen.deployment-deletion.delay: 0ms
    kubernetes.operator.bluegreen.reconciliation.reschedule-interval: 15s
  template:
    spec:
      flinkConfiguration:
        execution.checkpointing.dir: s3://flink/checkpoints
        execution.checkpointing.interval: 30s
        execution.checkpointing.savepoint-dir: s3://flink/savepoints
        fs.s3a.access.key: flinklab
        fs.s3a.aws.credentials.provider: org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
        fs.s3a.connection.ssl.enabled: "false"
        fs.s3a.endpoint: http://host.docker.internal:9000
        fs.s3a.path.style.access: "true"
        fs.s3a.secret.key: flinklab123
        s3.access-key: flinklab
        s3.endpoint: http://host.docker.internal:9000
        s3.path.style.access: "true"
        s3.secret-key: flinklab123
      flinkVersion: v2_2
      image: flinklab/p03-vehicle-alert:dev-green
      imagePullPolicy: IfNotPresent
      job:
        args:
        - --kafka-bootstrap
        - host.docker.internal:9095
        - --clickhouse-url
        - http://host.docker.internal:8123/
        - --clickhouse-user
        - flinklab
        - --clickhouse-password
        - flinklab123
        - --group-id
        - p03-vehicle-alerts-k8s
        - --events-topic
        - vehicle.events
        - --alerts-topic
        - vehicle.alerts
        - --control-topic
        - vehicle.pattern.control
        entryClass: com.flywhl.flinklab.p03.VehicleAlertJob
        jarURI: local:///opt/flink/usrlib/p03-vehicle-monitoring.jar
        parallelism: 1
        state: running
        upgradeMode: savepoint
      jobManager:
        resource:
          cpu: 0.5
          memory: 1024m
      podTemplate:
        spec:
          containers:
          - env:
            - name: ENABLE_BUILT_IN_PLUGINS
              value: flink-s3-fs-hadoop-2.2.1.jar
            name: flink-main-container
      serviceAccount: flink
      taskManager:
        replicas: 1
        resource:
          cpu: 1
          memory: 2048m
status:
  abortTimestamp: "2026-07-18T06:09:41.935Z"
  blueGreenState: TRANSITIONING_TO_GREEN
  deploymentReadyTimestamp: "2026-07-18T06:02:16.253335025Z"
  jobStatus:
    checkpointInfo:
      lastPeriodicCheckpointTimestamp: 0
    savepointInfo:
      lastPeriodicSavepointTimestamp: 0
      savepointHistory: []
    state: RECONCILING
  lastReconciledSpec: '{"spec":{"ingress":null,"template":{"metadata":null,"spec":{"job":{"jarURI":"local:///opt/flink/usrlib/p03-vehicle-monitoring.jar","parallelism":1,"entryClass":"com.flywhl.flinklab.p03.VehicleAlertJob","args":["--kafka-bootstrap","host.docker.internal:9095","--clickhouse-url","http://host.docker.internal:8123/","--clickhouse-user","flinklab","--clickhouse-password","flinklab123","--group-id","p03-vehicle-alerts-k8s","--events-topic","vehicle.events","--alerts-topic","vehicle.alerts","--control-topic","vehicle.pattern.control"],"state":"running","savepointTriggerNonce":null,"initialSavepointPath":null,"checkpointTriggerNonce":null,"upgradeMode":"savepoint","allowNonRestoredState":null,"savepointRedeployNonce":null,"autoscalerResetNonce":null},"restartNonce":null,"flinkConfiguration":{"execution.checkpointing.dir":"s3://flink/checkpoints","execution.checkpointing.interval":"30s","execution.checkpointing.savepoint-dir":"s3://flink/savepoints","fs.s3a.access.key":"flinklab","fs.s3a.aws.credentials.provider":"org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider","fs.s3a.connection.ssl.enabled":"false","fs.s3a.endpoint":"http://host.docker.internal:9000","fs.s3a.path.style.access":"true","fs.s3a.secret.key":"flinklab123","s3.access-key":"flinklab","s3.endpoint":"http://host.docker.internal:9000","s3.path.style.access":"true","s3.secret-key":"flinklab123"},"image":"flinklab/p03-vehicle-alert:dev-green","imagePullPolicy":"IfNotPresent","serviceAccount":"flink","flinkVersion":"v2_2","ingress":null,"podTemplate":{"spec":{"containers":[{"env":[{"name":"ENABLE_BUILT_IN_PLUGINS","value":"flink-s3-fs-hadoop-2.2.1.jar"}],"name":"flink-main-container"}]}},"jobManager":{"resource":{"cpu":0.5,"memory":"1024m","ephemeralStorage":null},"replicas":1,"podTemplate":null},"taskManager":{"resource":{"cpu":1.0,"memory":"2048m","ephemeralStorage":null},"replicas":1,"podTemplate":null},"logConfiguration":null,"mode":null}},"configuration":{"kubernetes.operator.bluegreen.abort.grace-period":"8
    min","kubernetes.operator.bluegreen.deployment-deletion.delay":"0ms","kubernetes.operator.bluegreen.reconciliation.reschedule-interval":"15s"}}}'
  lastReconciledTimestamp: "2026-07-18T06:02:16.253349359Z"
  savepointTriggerId: f98c4b73a3e509b565f01db5da3ad313
```

```
NAME                         JOB STATUS   LIFECYCLE STATE
p03-vehicle-alert-bg-blue    RUNNING      STABLE
p03-vehicle-alert-bg-green   RUNNING      STABLE
```

### Snapshot during TRANSITIONING_TO_GREEN

UTC: 2026-07-18T06:02:29Z

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkBlueGreenDeployment
metadata:
  annotations:
    meta.helm.sh/release-name: p03-vehicle-alert
    meta.helm.sh/release-namespace: flink
  creationTimestamp: "2026-07-18T05:59:29Z"
  generation: 2
  labels:
    app.kubernetes.io/managed-by: Helm
    app.kubernetes.io/name: p03-vehicle-alert
    app.kubernetes.io/part-of: flink-learning-lab
  name: p03-vehicle-alert-bg
  namespace: flink
  resourceVersion: "2824"
  uid: 7f19b8ab-0420-4706-8379-0be9b27126f1
spec:
  configuration:
    kubernetes.operator.bluegreen.abort.grace-period: 8 min
    kubernetes.operator.bluegreen.deployment-deletion.delay: 0ms
    kubernetes.operator.bluegreen.reconciliation.reschedule-interval: 15s
  template:
    spec:
      flinkConfiguration:
        execution.checkpointing.dir: s3://flink/checkpoints
        execution.checkpointing.interval: 30s
        execution.checkpointing.savepoint-dir: s3://flink/savepoints
        fs.s3a.access.key: flinklab
        fs.s3a.aws.credentials.provider: org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
        fs.s3a.connection.ssl.enabled: "false"
        fs.s3a.endpoint: http://host.docker.internal:9000
        fs.s3a.path.style.access: "true"
        fs.s3a.secret.key: flinklab123
        s3.access-key: flinklab
        s3.endpoint: http://host.docker.internal:9000
        s3.path.style.access: "true"
        s3.secret-key: flinklab123
      flinkVersion: v2_2
      image: flinklab/p03-vehicle-alert:dev-green
      imagePullPolicy: IfNotPresent
      job:
        args:
        - --kafka-bootstrap
        - host.docker.internal:9095
        - --clickhouse-url
        - http://host.docker.internal:8123/
        - --clickhouse-user
        - flinklab
        - --clickhouse-password
        - flinklab123
        - --group-id
        - p03-vehicle-alerts-k8s
        - --events-topic
        - vehicle.events
        - --alerts-topic
        - vehicle.alerts
        - --control-topic
        - vehicle.pattern.control
        entryClass: com.flywhl.flinklab.p03.VehicleAlertJob
        jarURI: local:///opt/flink/usrlib/p03-vehicle-monitoring.jar
        parallelism: 1
        state: running
        upgradeMode: savepoint
      jobManager:
        resource:
          cpu: 0.5
          memory: 1024m
      podTemplate:
        spec:
          containers:
          - env:
            - name: ENABLE_BUILT_IN_PLUGINS
              value: flink-s3-fs-hadoop-2.2.1.jar
            name: flink-main-container
      serviceAccount: flink
      taskManager:
        replicas: 1
        resource:
          cpu: 1
          memory: 2048m
status:
  abortTimestamp: "2026-07-18T06:09:41.935Z"
  blueGreenState: TRANSITIONING_TO_GREEN
  deploymentReadyTimestamp: "2026-07-18T06:02:16.253335025Z"
  jobStatus:
    checkpointInfo:
      lastPeriodicCheckpointTimestamp: 0
    savepointInfo:
      lastPeriodicSavepointTimestamp: 0
      savepointHistory: []
    state: RECONCILING
  lastReconciledSpec: '{"spec":{"ingress":null,"template":{"metadata":null,"spec":{"job":{"jarURI":"local:///opt/flink/usrlib/p03-vehicle-monitoring.jar","parallelism":1,"entryClass":"com.flywhl.flinklab.p03.VehicleAlertJob","args":["--kafka-bootstrap","host.docker.internal:9095","--clickhouse-url","http://host.docker.internal:8123/","--clickhouse-user","flinklab","--clickhouse-password","flinklab123","--group-id","p03-vehicle-alerts-k8s","--events-topic","vehicle.events","--alerts-topic","vehicle.alerts","--control-topic","vehicle.pattern.control"],"state":"running","savepointTriggerNonce":null,"initialSavepointPath":null,"checkpointTriggerNonce":null,"upgradeMode":"savepoint","allowNonRestoredState":null,"savepointRedeployNonce":null,"autoscalerResetNonce":null},"restartNonce":null,"flinkConfiguration":{"execution.checkpointing.dir":"s3://flink/checkpoints","execution.checkpointing.interval":"30s","execution.checkpointing.savepoint-dir":"s3://flink/savepoints","fs.s3a.access.key":"flinklab","fs.s3a.aws.credentials.provider":"org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider","fs.s3a.connection.ssl.enabled":"false","fs.s3a.endpoint":"http://host.docker.internal:9000","fs.s3a.path.style.access":"true","fs.s3a.secret.key":"flinklab123","s3.access-key":"flinklab","s3.endpoint":"http://host.docker.internal:9000","s3.path.style.access":"true","s3.secret-key":"flinklab123"},"image":"flinklab/p03-vehicle-alert:dev-green","imagePullPolicy":"IfNotPresent","serviceAccount":"flink","flinkVersion":"v2_2","ingress":null,"podTemplate":{"spec":{"containers":[{"env":[{"name":"ENABLE_BUILT_IN_PLUGINS","value":"flink-s3-fs-hadoop-2.2.1.jar"}],"name":"flink-main-container"}]}},"jobManager":{"resource":{"cpu":0.5,"memory":"1024m","ephemeralStorage":null},"replicas":1,"podTemplate":null},"taskManager":{"resource":{"cpu":1.0,"memory":"2048m","ephemeralStorage":null},"replicas":1,"podTemplate":null},"logConfiguration":null,"mode":null}},"configuration":{"kubernetes.operator.bluegreen.abort.grace-period":"8
    min","kubernetes.operator.bluegreen.deployment-deletion.delay":"0ms","kubernetes.operator.bluegreen.reconciliation.reschedule-interval":"15s"}}}'
  lastReconciledTimestamp: "2026-07-18T06:02:16.253349359Z"
  savepointTriggerId: f98c4b73a3e509b565f01db5da3ad313
```

```
NAME                         JOB STATUS   LIFECYCLE STATE
p03-vehicle-alert-bg-green   RUNNING      STABLE
```

## Post-transition status

UTC: 2026-07-18T06:02:39Z

- Observed migration: `ACTIVE_BLUE` → `ACTIVE_GREEN`

### FlinkBlueGreenDeployment status

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkBlueGreenDeployment
metadata:
  annotations:
    meta.helm.sh/release-name: p03-vehicle-alert
    meta.helm.sh/release-namespace: flink
  creationTimestamp: "2026-07-18T05:59:29Z"
  generation: 2
  labels:
    app.kubernetes.io/managed-by: Helm
    app.kubernetes.io/name: p03-vehicle-alert
    app.kubernetes.io/part-of: flink-learning-lab
  name: p03-vehicle-alert-bg
  namespace: flink
  resourceVersion: "2877"
  uid: 7f19b8ab-0420-4706-8379-0be9b27126f1
spec:
  configuration:
    kubernetes.operator.bluegreen.abort.grace-period: 8 min
    kubernetes.operator.bluegreen.deployment-deletion.delay: 0ms
    kubernetes.operator.bluegreen.reconciliation.reschedule-interval: 15s
  template:
    spec:
      flinkConfiguration:
        execution.checkpointing.dir: s3://flink/checkpoints
        execution.checkpointing.interval: 30s
        execution.checkpointing.savepoint-dir: s3://flink/savepoints
        fs.s3a.access.key: flinklab
        fs.s3a.aws.credentials.provider: org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
        fs.s3a.connection.ssl.enabled: "false"
        fs.s3a.endpoint: http://host.docker.internal:9000
        fs.s3a.path.style.access: "true"
        fs.s3a.secret.key: flinklab123
        s3.access-key: flinklab
        s3.endpoint: http://host.docker.internal:9000
        s3.path.style.access: "true"
        s3.secret-key: flinklab123
      flinkVersion: v2_2
      image: flinklab/p03-vehicle-alert:dev-green
      imagePullPolicy: IfNotPresent
      job:
        args:
        - --kafka-bootstrap
        - host.docker.internal:9095
        - --clickhouse-url
        - http://host.docker.internal:8123/
        - --clickhouse-user
        - flinklab
        - --clickhouse-password
        - flinklab123
        - --group-id
        - p03-vehicle-alerts-k8s
        - --events-topic
        - vehicle.events
        - --alerts-topic
        - vehicle.alerts
        - --control-topic
        - vehicle.pattern.control
        entryClass: com.flywhl.flinklab.p03.VehicleAlertJob
        jarURI: local:///opt/flink/usrlib/p03-vehicle-monitoring.jar
        parallelism: 1
        state: running
        upgradeMode: savepoint
      jobManager:
        resource:
          cpu: 0.5
          memory: 1024m
      podTemplate:
        spec:
          containers:
          - env:
            - name: ENABLE_BUILT_IN_PLUGINS
              value: flink-s3-fs-hadoop-2.2.1.jar
            name: flink-main-container
      serviceAccount: flink
      taskManager:
        replicas: 1
        resource:
          cpu: 1
          memory: 2048m
status:
  abortTimestamp: "1970-01-01T00:00:00Z"
  blueGreenState: ACTIVE_GREEN
  deploymentReadyTimestamp: "1970-01-01T00:00:00Z"
  jobStatus:
    checkpointInfo:
      lastPeriodicCheckpointTimestamp: 0
    savepointInfo:
      lastPeriodicSavepointTimestamp: 0
      savepointHistory: []
    state: RUNNING
  lastReconciledSpec: '{"spec":{"ingress":null,"template":{"metadata":null,"spec":{"job":{"jarURI":"local:///opt/flink/usrlib/p03-vehicle-monitoring.jar","parallelism":1,"entryClass":"com.flywhl.flinklab.p03.VehicleAlertJob","args":["--kafka-bootstrap","host.docker.internal:9095","--clickhouse-url","http://host.docker.internal:8123/","--clickhouse-user","flinklab","--clickhouse-password","flinklab123","--group-id","p03-vehicle-alerts-k8s","--events-topic","vehicle.events","--alerts-topic","vehicle.alerts","--control-topic","vehicle.pattern.control"],"state":"running","savepointTriggerNonce":null,"initialSavepointPath":null,"checkpointTriggerNonce":null,"upgradeMode":"savepoint","allowNonRestoredState":null,"savepointRedeployNonce":null,"autoscalerResetNonce":null},"restartNonce":null,"flinkConfiguration":{"execution.checkpointing.dir":"s3://flink/checkpoints","execution.checkpointing.interval":"30s","execution.checkpointing.savepoint-dir":"s3://flink/savepoints","fs.s3a.access.key":"flinklab","fs.s3a.aws.credentials.provider":"org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider","fs.s3a.connection.ssl.enabled":"false","fs.s3a.endpoint":"http://host.docker.internal:9000","fs.s3a.path.style.access":"true","fs.s3a.secret.key":"flinklab123","s3.access-key":"flinklab","s3.endpoint":"http://host.docker.internal:9000","s3.path.style.access":"true","s3.secret-key":"flinklab123"},"image":"flinklab/p03-vehicle-alert:dev-green","imagePullPolicy":"IfNotPresent","serviceAccount":"flink","flinkVersion":"v2_2","ingress":null,"podTemplate":{"spec":{"containers":[{"env":[{"name":"ENABLE_BUILT_IN_PLUGINS","value":"flink-s3-fs-hadoop-2.2.1.jar"}],"name":"flink-main-container"}]}},"jobManager":{"resource":{"cpu":0.5,"memory":"1024m","ephemeralStorage":null},"replicas":1,"podTemplate":null},"taskManager":{"resource":{"cpu":1.0,"memory":"2048m","ephemeralStorage":null},"replicas":1,"podTemplate":null},"logConfiguration":null,"mode":null}},"configuration":{"kubernetes.operator.bluegreen.abort.grace-period":"8
    min","kubernetes.operator.bluegreen.deployment-deletion.delay":"0ms","kubernetes.operator.bluegreen.reconciliation.reschedule-interval":"15s"}}}'
  lastReconciledTimestamp: "2026-07-18T06:02:36.939881145Z"
```

### Child FlinkDeployments

```
NAME                         JOB STATUS   LIFECYCLE STATE
p03-vehicle-alert-bg-green   RUNNING      STABLE
```

```yaml
apiVersion: v1
items:
- apiVersion: flink.apache.org/v1beta1
  kind: FlinkDeployment
  metadata:
    creationTimestamp: "2026-07-18T06:01:41Z"
    finalizers:
    - flinkdeployments.flink.apache.org/finalizer
    generation: 1
    labels:
      flink/blue-green-deployment-type: GREEN
    name: p03-vehicle-alert-bg-green
    namespace: flink
    ownerReferences:
    - apiVersion: flink.apache.org/v1beta1
      blockOwnerDeletion: true
      controller: false
      kind: FlinkBlueGreenDeployment
      name: p03-vehicle-alert-bg
      uid: 7f19b8ab-0420-4706-8379-0be9b27126f1
    resourceVersion: "2825"
    uid: 19ede3fb-9333-435d-975f-f6b370543d4d
  spec:
    flinkConfiguration:
      execution.checkpointing.dir: s3://flink/checkpoints
      execution.checkpointing.interval: 30s
      execution.checkpointing.savepoint-dir: s3://flink/savepoints
      fs.s3a.access.key: flinklab
      fs.s3a.aws.credentials.provider: org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
      fs.s3a.connection.ssl.enabled: "false"
      fs.s3a.endpoint: http://host.docker.internal:9000
      fs.s3a.path.style.access: "true"
      fs.s3a.secret.key: flinklab123
      s3.access-key: flinklab
      s3.endpoint: http://host.docker.internal:9000
      s3.path.style.access: "true"
      s3.secret-key: flinklab123
    flinkVersion: v2_2
    image: flinklab/p03-vehicle-alert:dev-green
    imagePullPolicy: IfNotPresent
    job:
      args:
      - --kafka-bootstrap
      - host.docker.internal:9095
      - --clickhouse-url
      - http://host.docker.internal:8123/
      - --clickhouse-user
      - flinklab
      - --clickhouse-password
      - flinklab123
      - --group-id
      - p03-vehicle-alerts-k8s
      - --events-topic
      - vehicle.events
      - --alerts-topic
      - vehicle.alerts
      - --control-topic
      - vehicle.pattern.control
      entryClass: com.flywhl.flinklab.p03.VehicleAlertJob
      initialSavepointPath: s3://flink/savepoints/savepoint-66f218-64c1320e7a1f
      jarURI: local:///opt/flink/usrlib/p03-vehicle-monitoring.jar
      parallelism: 1
      state: running
      upgradeMode: savepoint
    jobManager:
      replicas: 1
      resource:
        cpu: 0.5
        memory: 1024m
    podTemplate:
      spec:
        containers:
        - env:
          - name: ENABLE_BUILT_IN_PLUGINS
            value: flink-s3-fs-hadoop-2.2.1.jar
          name: flink-main-container
    serviceAccount: flink
    taskManager:
      replicas: 1
      resource:
        cpu: 1
        memory: 2048m
  status:
    clusterInfo:
      flink-revision: 450c63e @ 2026-05-03T10:10:44+02:00
      flink-version: 2.2.1
      state-size: "141008"
      total-cpu: "1.5"
      total-memory: "3221225472"
    conditions:
    - lastTransitionTime: "2026-07-18T06:02:16.262034769Z"
      message: Job status RUNNING
      reason: Running
      status: "True"
      type: Running
    jobManagerDeploymentStatus: READY
    jobStatus:
      checkpointInfo:
        lastPeriodicCheckpointTimestamp: 0
      jobId: f3389b99e4396c634593c037845b9e03
      jobName: p03-vehicle-alert
      savepointInfo:
        lastPeriodicSavepointTimestamp: 0
        savepointHistory: []
      startTime: "1784354512042"
      state: RUNNING
      updateTime: "1784354536138"
      upgradeSavepointPath: s3://flink/savepoints/savepoint-66f218-64c1320e7a1f
    lifecycleState: STABLE
    observedGeneration: 1
    reconciliationStatus:
      lastReconciledSpec: '{"spec":{"job":{"jarURI":"local:///opt/flink/usrlib/p03-vehicle-monitoring.jar","parallelism":1,"entryClass":"com.flywhl.flinklab.p03.VehicleAlertJob","args":["--kafka-bootstrap","host.docker.internal:9095","--clickhouse-url","http://host.docker.internal:8123/","--clickhouse-user","flinklab","--clickhouse-password","flinklab123","--group-id","p03-vehicle-alerts-k8s","--events-topic","vehicle.events","--alerts-topic","vehicle.alerts","--control-topic","vehicle.pattern.control"],"state":"running","savepointTriggerNonce":null,"initialSavepointPath":"s3://flink/savepoints/savepoint-66f218-64c1320e7a1f","checkpointTriggerNonce":null,"upgradeMode":"savepoint","allowNonRestoredState":null,"savepointRedeployNonce":null,"autoscalerResetNonce":null},"restartNonce":null,"flinkConfiguration":{"execution.checkpointing.dir":"s3://flink/checkpoints","execution.checkpointing.interval":"30s","execution.checkpointing.savepoint-dir":"s3://flink/savepoints","fs.s3a.access.key":"flinklab","fs.s3a.aws.credentials.provider":"org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider","fs.s3a.connection.ssl.enabled":"false","fs.s3a.endpoint":"http://host.docker.internal:9000","fs.s3a.path.style.access":"true","fs.s3a.secret.key":"flinklab123","s3.access-key":"flinklab","s3.endpoint":"http://host.docker.internal:9000","s3.path.style.access":"true","s3.secret-key":"flinklab123"},"image":"flinklab/p03-vehicle-alert:dev-green","imagePullPolicy":"IfNotPresent","serviceAccount":"flink","flinkVersion":"v2_2","ingress":null,"podTemplate":{"spec":{"containers":[{"env":[{"name":"ENABLE_BUILT_IN_PLUGINS","value":"flink-s3-fs-hadoop-2.2.1.jar"}],"name":"flink-main-container"}]}},"jobManager":{"resource":{"cpu":0.5,"memory":"1024m","ephemeralStorage":null},"replicas":1,"podTemplate":null},"taskManager":{"resource":{"cpu":1.0,"memory":"2048m","ephemeralStorage":null},"replicas":1,"podTemplate":null},"logConfiguration":null,"mode":null},"resource_metadata":{"apiVersion":"flink.apache.org/v1beta1","firstDeployment":true}}'
      lastStableSpec: '{"spec":{"job":{"jarURI":"local:///opt/flink/usrlib/p03-vehicle-monitoring.jar","parallelism":1,"entryClass":"com.flywhl.flinklab.p03.VehicleAlertJob","args":["--kafka-bootstrap","host.docker.internal:9095","--clickhouse-url","http://host.docker.internal:8123/","--clickhouse-user","flinklab","--clickhouse-password","flinklab123","--group-id","p03-vehicle-alerts-k8s","--events-topic","vehicle.events","--alerts-topic","vehicle.alerts","--control-topic","vehicle.pattern.control"],"state":"running","savepointTriggerNonce":null,"initialSavepointPath":"s3://flink/savepoints/savepoint-66f218-64c1320e7a1f","checkpointTriggerNonce":null,"upgradeMode":"savepoint","allowNonRestoredState":null,"savepointRedeployNonce":null,"autoscalerResetNonce":null},"restartNonce":null,"flinkConfiguration":{"execution.checkpointing.dir":"s3://flink/checkpoints","execution.checkpointing.interval":"30s","execution.checkpointing.savepoint-dir":"s3://flink/savepoints","fs.s3a.access.key":"flinklab","fs.s3a.aws.credentials.provider":"org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider","fs.s3a.connection.ssl.enabled":"false","fs.s3a.endpoint":"http://host.docker.internal:9000","fs.s3a.path.style.access":"true","fs.s3a.secret.key":"flinklab123","s3.access-key":"flinklab","s3.endpoint":"http://host.docker.internal:9000","s3.path.style.access":"true","s3.secret-key":"flinklab123"},"image":"flinklab/p03-vehicle-alert:dev-green","imagePullPolicy":"IfNotPresent","serviceAccount":"flink","flinkVersion":"v2_2","ingress":null,"podTemplate":{"spec":{"containers":[{"env":[{"name":"ENABLE_BUILT_IN_PLUGINS","value":"flink-s3-fs-hadoop-2.2.1.jar"}],"name":"flink-main-container"}]}},"jobManager":{"resource":{"cpu":0.5,"memory":"1024m","ephemeralStorage":null},"replicas":1,"podTemplate":null},"taskManager":{"resource":{"cpu":1.0,"memory":"2048m","ephemeralStorage":null},"replicas":1,"podTemplate":null},"logConfiguration":null,"mode":null},"resource_metadata":{"apiVersion":"flink.apache.org/v1beta1","firstDeployment":true}}'
      reconciliationTimestamp: 1784354502149
      state: DEPLOYED
    taskManager:
      labelSelector: component=taskmanager,app=p03-vehicle-alert-bg-green
      replicas: 1
kind: List
metadata:
  resourceVersion: ""
```

### Events

```
LAST SEEN   TYPE      REASON              OBJECT                                            MESSAGE
12m         Normal    Scheduled           pod/p03-vehicle-alert-bg-blue-868c48cf67-sm79r    Successfully assigned flink/p03-vehicle-alert-bg-blue-868c48cf67-sm79r to orbstack
42s         Normal    Scheduled           pod/p03-vehicle-alert-bg-green-taskmanager-1-1    Successfully assigned flink/p03-vehicle-alert-bg-green-taskmanager-1-1 to orbstack
57s         Normal    Scheduled           pod/p03-vehicle-alert-bg-green-b8b5d47d4-9l682    Successfully assigned flink/p03-vehicle-alert-bg-green-b8b5d47d4-9l682 to orbstack
21m         Normal    Scheduled           pod/p03-vehicle-alert-bg-blue-868c48cf67-6qgrr    Successfully assigned flink/p03-vehicle-alert-bg-blue-868c48cf67-6qgrr to orbstack
2m55s       Normal    Scheduled           pod/p03-vehicle-alert-bg-blue-taskmanager-1-1     Successfully assigned flink/p03-vehicle-alert-bg-blue-taskmanager-1-1 to orbstack
11m         Normal    Scheduled           pod/p03-vehicle-alert-bg-blue-taskmanager-1-1     Successfully assigned flink/p03-vehicle-alert-bg-blue-taskmanager-1-1 to orbstack
3m9s        Normal    Scheduled           pod/p03-vehicle-alert-bg-blue-868c48cf67-fhj4l    Successfully assigned flink/p03-vehicle-alert-bg-blue-868c48cf67-fhj4l to orbstack
21m         Normal    Submit              flinkdeployment/p03-vehicle-alert-bg-blue         Starting deployment
21m         Normal    ScalingReplicaSet   deployment/p03-vehicle-alert-bg-blue              Scaled up replica set p03-vehicle-alert-bg-blue-868c48cf67 from 0 to 1
21m         Normal    SuccessfulCreate    replicaset/p03-vehicle-alert-bg-blue-868c48cf67   Created pod: p03-vehicle-alert-bg-blue-868c48cf67-6qgrr
21m         Normal    JobStatusChanged    flinkdeployment/p03-vehicle-alert-bg-blue         Job status changed from RECONCILING to INITIALIZING
20m         Warning   Unhealthy           pod/p03-vehicle-alert-bg-blue-868c48cf67-6qgrr    Startup probe failed: Get "http://192.168.194.11:8081/config": dial tcp 192.168.194.11:8081: connect: connection refused
20m         Warning   CrashLoopBackOff    flinkdeployment/p03-vehicle-alert-bg-blue         [flink-main-container] back-off 20s restarting failed container=flink-main-container pod=p03-vehicle-alert-bg-blue-868c48cf67-6qgrr_flink(651bb1b3-de12-4121-837e-aedbd5ab4627)
19m         Warning   CrashLoopBackOff    flinkdeployment/p03-vehicle-alert-bg-blue         [flink-main-container] back-off 40s restarting failed container=flink-main-container pod=p03-vehicle-alert-bg-blue-868c48cf67-6qgrr_flink(651bb1b3-de12-4121-837e-aedbd5ab4627)
18m         Warning   CrashLoopBackOff    flinkdeployment/p03-vehicle-alert-bg-blue         [flink-main-container] back-off 1m20s restarting failed container=flink-main-container pod=p03-vehicle-alert-bg-blue-868c48cf67-6qgrr_flink(651bb1b3-de12-4121-837e-aedbd5ab4627)
16m         Warning   CrashLoopBackOff    flinkdeployment/p03-vehicle-alert-bg-blue         [flink-main-container] back-off 2m40s restarting failed container=flink-main-container pod=p03-vehicle-alert-bg-blue-868c48cf67-6qgrr_flink(651bb1b3-de12-4121-837e-aedbd5ab4627)
16m         Warning   BackOff             pod/p03-vehicle-alert-bg-blue-868c48cf67-6qgrr    Back-off restarting failed container flink-main-container in pod p03-vehicle-alert-bg-blue-868c48cf67-6qgrr_flink(651bb1b3-de12-4121-837e-aedbd5ab4627)
14m         Normal    Created             pod/p03-vehicle-alert-bg-blue-868c48cf67-6qgrr    Created container: flink-main-container
14m         Normal    Pulled              pod/p03-vehicle-alert-bg-blue-868c48cf67-6qgrr    Container image "flinklab/p03-vehicle-alert:dev" already present on machine
14m         Normal    Started             pod/p03-vehicle-alert-bg-blue-868c48cf67-6qgrr    Started container flink-main-container
13m         Warning   CrashLoopBackOff    flinkdeployment/p03-vehicle-alert-bg-blue         [flink-main-container] back-off 5m0s restarting failed container=flink-main-container pod=p03-vehicle-alert-bg-blue-868c48cf67-6qgrr_flink(651bb1b3-de12-4121-837e-aedbd5ab4627)
13m         Normal    SpecChanged         flinkdeployment/p03-vehicle-alert-bg-blue         UPGRADE change(s) detected (Diff: FlinkDeploymentSpec[job.state : running -> suspended, job.upgradeMode : stateless -> savepoint]), starting reconciliation.
12m         Warning   UpgradeFailed       flinkdeployment/p03-vehicle-alert-bg-blue         JobManager deployment is missing and HA metadata is not available to make stateful upgrades. It is possible that the job has finished or terminally failed, or the configmaps have been deleted.Manual restore required.
12m         Normal    Cleanup             flinkdeployment/p03-vehicle-alert-bg-blue         Cleaning up FlinkDeployment
12m         Normal    SuccessfulDelete    replicaset/p03-vehicle-alert-bg-blue-868c48cf67   Deleted pod: p03-vehicle-alert-bg-blue-868c48cf67-6qgrr
12m         Normal    ScalingReplicaSet   deployment/p03-vehicle-alert-bg-blue              Scaled down replica set p03-vehicle-alert-bg-blue-868c48cf67 from 1 to 0
12m         Normal    Submit              flinkdeployment/p03-vehicle-alert-bg-blue         Starting deployment
12m         Normal    ScalingReplicaSet   deployment/p03-vehicle-alert-bg-blue              Scaled up replica set p03-vehicle-alert-bg-blue-868c48cf67 from 0 to 1
12m         Normal    Pulled              pod/p03-vehicle-alert-bg-blue-868c48cf67-sm79r    Container image "flinklab/p03-vehicle-alert:dev" already present on machine
12m         Normal    Created             pod/p03-vehicle-alert-bg-blue-868c48cf67-sm79r    Created container: flink-main-container
12m         Normal    SuccessfulCreate    replicaset/p03-vehicle-alert-bg-blue-868c48cf67   Created pod: p03-vehicle-alert-bg-blue-868c48cf67-sm79r
12m         Normal    Started             pod/p03-vehicle-alert-bg-blue-868c48cf67-sm79r    Started container flink-main-container
11m         Warning   Unhealthy           pod/p03-vehicle-alert-bg-blue-868c48cf67-sm79r    Startup probe failed: Get "http://192.168.194.12:8081/config": dial tcp 192.168.194.12:8081: connect: connection refused
11m         Normal    Created             pod/p03-vehicle-alert-bg-blue-taskmanager-1-1     Created container: flink-main-container
11m         Normal    Started             pod/p03-vehicle-alert-bg-blue-taskmanager-1-1     Started container flink-main-container
11m         Normal    Pulled              pod/p03-vehicle-alert-bg-blue-taskmanager-1-1     Container image "flinklab/p03-vehicle-alert:dev" already present on machine
11m         Normal    JobStatusChanged    flinkdeployment/p03-vehicle-alert-bg-blue         Job status changed from RECONCILING to RESTARTING
6m43s       Warning   JobException        flinkdeployment/p03-vehicle-alert-bg-blue         org.apache.flink.util.FlinkException: Global failure triggered by OperatorCoordinator for 'Source: kafka-vehicle-events -> parse-vehicle-json -> Timestamps/Watermarks' (operator 46d1da3639fa719f605c845b064c5e2a)....
5m27s       Normal    Killing             pod/p03-vehicle-alert-bg-blue-taskmanager-1-1     Stopping container flink-main-container
4m43s       Warning   JobException        flinkdeployment/p03-vehicle-alert-bg-blue         org.apache.flink.util.FlinkException: Global failure triggered by OperatorCoordinator for 'Source: kafka-pattern-control -> parse-pattern-control' (operator 389491edbdab9d0ac4c2dbcd42257d61)....
3m57s       Normal    Suspended           flinkdeployment/p03-vehicle-alert-bg-blue         Suspending existing deployment.
3m57s       Normal    SpecChanged         flinkdeployment/p03-vehicle-alert-bg-blue         UPGRADE change(s) detected (Diff: FlinkDeploymentSpec[job.state : running -> suspended, job.upgradeMode : stateless -> savepoint]), starting reconciliation.
3m52s       Normal    JobStatusChanged    flinkdeployment/p03-vehicle-alert-bg-blue         Job status changed from CANCELLING to CANCELED
3m16s       Normal    Killing             pod/p03-vehicle-alert-bg-blue-868c48cf67-sm79r    Stopping container flink-main-container
3m16s       Normal    Cleanup             flinkdeployment/p03-vehicle-alert-bg-blue         Cleaning up FlinkDeployment
3m16s       Normal    SuccessfulDelete    replicaset/p03-vehicle-alert-bg-blue-868c48cf67   Deleted pod: p03-vehicle-alert-bg-blue-868c48cf67-sm79r
3m16s       Normal    ScalingReplicaSet   deployment/p03-vehicle-alert-bg-blue              Scaled down replica set p03-vehicle-alert-bg-blue-868c48cf67 from 1 to 0
3m10s       Normal    Submit              flinkdeployment/p03-vehicle-alert-bg-blue         Starting deployment
3m9s        Normal    SuccessfulCreate    replicaset/p03-vehicle-alert-bg-blue-868c48cf67   Created pod: p03-vehicle-alert-bg-blue-868c48cf67-fhj4l
3m9s        Normal    Pulled              pod/p03-vehicle-alert-bg-blue-868c48cf67-fhj4l    Container image "flinklab/p03-vehicle-alert:dev" already present on machine
3m9s        Normal    Created             pod/p03-vehicle-alert-bg-blue-868c48cf67-fhj4l    Created container: flink-main-container
3m9s        Normal    ScalingReplicaSet   deployment/p03-vehicle-alert-bg-blue              Scaled up replica set p03-vehicle-alert-bg-blue-868c48cf67 from 0 to 1
3m9s        Normal    Started             pod/p03-vehicle-alert-bg-blue-868c48cf67-fhj4l    Started container flink-main-container
3m2s        Warning   Unhealthy           pod/p03-vehicle-alert-bg-blue-868c48cf67-fhj4l    Startup probe failed: Get "http://192.168.194.16:8081/config": dial tcp 192.168.194.16:8081: connect: connection refused
2m55s       Normal    Pulled              pod/p03-vehicle-alert-bg-blue-taskmanager-1-1     Container image "flinklab/p03-vehicle-alert:dev" already present on machine
2m55s       Normal    Created             pod/p03-vehicle-alert-bg-blue-taskmanager-1-1     Created container: flink-main-container
2m55s       Normal    Started             pod/p03-vehicle-alert-bg-blue-taskmanager-1-1     Started container flink-main-container
2m51s       Normal    JobStatusChanged    flinkdeployment/p03-vehicle-alert-bg-blue         Job status changed from RECONCILING to CREATED
2m35s       Normal    JobStatusChanged    flinkdeployment/p03-vehicle-alert-bg-blue         Job status changed from CREATED to RUNNING
58s         Normal    Submit              flinkdeployment/p03-vehicle-alert-bg-green        Starting deployment
57s         Normal    SuccessfulCreate    replicaset/p03-vehicle-alert-bg-green-b8b5d47d4   Created pod: p03-vehicle-alert-bg-green-b8b5d47d4-9l682
57s         Normal    ScalingReplicaSet   deployment/p03-vehicle-alert-bg-green             Scaled up replica set p03-vehicle-alert-bg-green-b8b5d47d4 from 0 to 1
57s         Normal    Pulled              pod/p03-vehicle-alert-bg-green-b8b5d47d4-9l682    Container image "flinklab/p03-vehicle-alert:dev-green" already present on machine
57s         Normal    Created             pod/p03-vehicle-alert-bg-green-b8b5d47d4-9l682    Created container: flink-main-container
57s         Normal    Started             pod/p03-vehicle-alert-bg-green-b8b5d47d4-9l682    Started container flink-main-container
50s         Warning   Unhealthy           pod/p03-vehicle-alert-bg-green-b8b5d47d4-9l682    Startup probe failed: Get "http://192.168.194.22:8081/config": dial tcp 192.168.194.22:8081: connect: connection refused
42s         Normal    Pulled              pod/p03-vehicle-alert-bg-green-taskmanager-1-1    Container image "flinklab/p03-vehicle-alert:dev-green" already present on machine
42s         Normal    Started             pod/p03-vehicle-alert-bg-green-taskmanager-1-1    Started container flink-main-container
42s         Normal    Created             pod/p03-vehicle-alert-bg-green-taskmanager-1-1    Created container: flink-main-container
38s         Normal    JobStatusChanged    flinkdeployment/p03-vehicle-alert-bg-green        Job status changed from RECONCILING to CREATED
23s         Normal    ScalingReplicaSet   deployment/p03-vehicle-alert-bg-blue              Scaled down replica set p03-vehicle-alert-bg-blue-868c48cf67 from 1 to 0
23s         Normal    Killing             pod/p03-vehicle-alert-bg-blue-868c48cf67-fhj4l    Stopping container flink-main-container
23s         Normal    Cleanup             flinkdeployment/p03-vehicle-alert-bg-blue         Cleaning up FlinkDeployment
23s         Normal    JobStatusChanged    flinkdeployment/p03-vehicle-alert-bg-green        Job status changed from CREATED to RUNNING
23s         Normal    SuccessfulDelete    replicaset/p03-vehicle-alert-bg-blue-868c48cf67   Deleted pod: p03-vehicle-alert-bg-blue-868c48cf67-fhj4l
21s         Normal    Killing             pod/p03-vehicle-alert-bg-blue-taskmanager-1-1     Stopping container flink-main-container
```

## Result

- Finished UTC: 2026-07-18T06:02:39Z
- Outcome: **PASS** (`ACTIVE_BLUE` → `ACTIVE_GREEN`)

