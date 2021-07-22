from os import getenv
from datetime import datetime, timedelta
from airflow import DAG
#from airflow.operators.bash import BashOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor

default_args = {
    'owner' : 'Engage',
    'start_date' : datetime(2021, 7, 16),
    'depends_on_past' : False,
    'provide_context' : True
}
kube_config_path = '/usr/local/airflow/dags/kube_config.yaml'

# [START instantiate_dag]
with DAG(
        'SparkOperator-Demo-v1',
        default_args=default_args,
        schedule_interval=None,
        tags=['demo', 's3', 'ETL', 'SparkKubernetesOperator', 'k8s', 'json', 'comments']
) as dag :
    # [END instantiate_dag]

    # use spark-on-k8s to operate against the data
    # containerized spark application
    # yaml definition to trigger process

    spark_operator_demo = SparkKubernetesOperator(
        task_id='sparkoperatordemo',
        namespace='mwaa',
        kubernetes_conn_id='aurigo-microservices-dev',
        application_file="""
            {
    "apiVersion": "sparkoperator.k8s.io/v1beta2",
    "kind": "SparkApplication",
    "metadata": {
        "name": "test-spark-sample",
        "namespace": "mwaa"
    },
    "spec": {
        "driver": {
            "coreLimit": "1800m",
            "cores": 1,
            "labels": {
                "version": "3.1.1"
            },
            "memory": "1800m",
            "serviceAccount": "aurigo-dev-spark-spark",
            "volumeMounts": [
                {
                    "mountPath": "/tmp",
                    "name": "test-volume"
                }
            ]
        },
        "executor": {
            "cores": 1,
            "instances": 1,
            "labels": {
                "version": "3.1.1"
            },
            "memory": "1200m",
            "volumeMounts": [
                {
                    "mountPath": "/tmp",
                    "name": "test-volume"
                }
            ]
        },
        "image": "gcr.io/spark-operator/spark-py:v3.1.1",
        "imagePullPolicy": "IfNotPresent",
        "mainApplicationFile": "local:///opt/spark/examples/src/main/python/pi.py",
        "mode": "cluster",
        "restartPolicy": {
            "onFailureRetries": 1,
            "onFailureRetryInterval": 10,
            "onSubmissionFailureRetries": 1,
            "onSubmissionFailureRetryInterval": 20,
            "type": "OnFailure"
        },
        "sparkVersion": "3.1.1",
        "type": "Python",
        "volumes": [
            {
                "hostPath": {
                    "path": "/tmp",
                    "type": "Directory"
                },
                "name": "test-volume"
            }
        ]
    }
}
""",
        do_xcom_push=True,
        dag=dag
    )
    # [START task_sequence]
    spark_operator_demo
# [END task_sequence]
